import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, S3StoreBackendDefaults
from sns.slack_alert import send_message

def get_gx_context(bucket_name):
    """
    Great Expectations Data Context를 설정하고 반환
        store_backends : gx의 운영에 필요한 요소들 혹은 산출물들을 어디에 저장할지
        data_docs_sites : 결과 docs 파일을 어디에 저장할지
        datasources : 어떤 엔진을 활용할지
    """
    data_context_config = DataContextConfig(
        store_backends={
            "expectations_store": {"class_name": "ExpectationsStore"},
            "validations_store": {"class_name": "ValidationsStore", "store_backend": {"class_name": "TupleS3StoreBackend", "bucket": bucket_name, "prefix": "gx/validations"}},
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "checkpoint_store": {"class_name": "CheckpointStore", "store_backend": {"class_name": "TupleS3StoreBackend", "bucket": bucket_name, "prefix": "gx/checkpoints"}},
        },
        data_docs_sites={
            "s3_site": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": bucket_name,
                    "prefix": "gx/data_docs",
                },
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            }
        },
        datasources={
            "spark_s3_raw_news": {
                "class_name": "Datasource",
                "execution_engine": {"class_name": "SparkDFExecutionEngine"},
                "data_connectors": {
                    "runtime_data_connector": {
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": ["default_identifier_name"]
                    }
                }
            }
        },
    )
    return BaseDataContext(project_config=data_context_config)

def build_critical_payload(data_asset_name, failed_expectation, data_docs_path):
    """Slack - 크리티컬"""
    return {
        "text": f":rotating_light: [CRITICAL] Data Quality Check Failed for `{data_asset_name}`",
        "attachments": [{
            "color": "#FF0000",
            "fields": [
                {"title": "Status", "value": "Pipeline Stopped", "short": False},
                {"title": "Failed Expectation", "value": f"`{failed_expectation}`", "short": False},
                {"title": "Data Docs", "value": f"<{data_docs_path}|Click to view report>", "short": False}
            ]
        }]
    }

def build_warning_payload(data_asset_name, quarantined_count, passed_count, failed_expectations, quarantine_path, data_docs_path):
    """Slack - 경고"""
    failed_rules = "\n".join([f"- `{exp['expectation_config']['expectation_type']}` on column `{exp['expectation_config']['kwargs']['column']}`" for exp in failed_expectations])
    return {
        "text": f":warning: [WARNING] Data Quality Issues Found in `{data_asset_name}`",
        "attachments": [{
            "color": "#FFA500",
            "fields": [
                {"title": "Status", "value": "Bad data quarantined. Pipeline continues.", "short": False},
                {"title": "Quarantined Rows", "value": str(quarantined_count), "short": True},
                {"title": "Passed Rows", "value": str(passed_count), "short": True},
                {"title": "Failed Rules", "value": failed_rules, "short": False},
                {"title": "Quarantine Location", "value": f"`{quarantine_path}`", "short": False},
                {"title": "Data Docs", "value": f"<{data_docs_path}|Click to view report>", "short": False}
            ]
        }]
    }

def quarantine_failed_rows(df, failed_expectations):
    """Non-critical 검증에 실패한 행들을 격리

    뉴스 스위트 기준 지원 항목:
    - expect_column_values_to_match_regex (link URL 정규식)
    - expect_column_values_to_match_strftime_format (post_time: "%Y-%m-%d %H:%M:%S")
    - expect_column_values_to_not_be_null (Non-critical로 사용되는 경우에만 격리)

    테이블 레벨(expect_table_columns_to_match_ordered_list)은 행 단위 격리가 불가능하므로 제외합니다.
    """

    # 각 행에 대해 실패한 규칙을 기록할 컬럼 추가
    df_with_errors = df.withColumn("__gx_error_rules__", lit(""))

    for result in failed_expectations:
        expectation_type = result["expectation_config"]["expectation_type"]
        kwargs = result["expectation_config"].get("kwargs", {})

        # 테이블 레벨 기대치는 행 단위 격리 불가 → 스킵
        if expectation_type == "expect_table_columns_to_match_ordered_list":
            continue

        # 어떤 컬럼인지 확인
        column = kwargs.get("column")
        if not column:
            # 컬럼이 명시되지 않은 기대치는 행 단위로 처리하지 않음
            continue

        condition = None
        if expectation_type == "expect_column_values_to_match_regex":
            regex = kwargs.get("regex", ".*")
            condition = ~col(column).cast("string").rlike(regex)
        elif expectation_type == "expect_column_values_to_match_strftime_format":
            format_regex = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$"
            condition = ~col(column).cast("string").rlike(format_regex)
        elif expectation_type == "expect_column_values_to_not_be_null":
            condition = col(column).isNull()

        if condition is not None:
            df_with_errors = df_with_errors.withColumn(
                "__gx_error_rules__",
                when(condition, col("__gx_error_rules__") + f"{expectation_type};").otherwise(col("__gx_error_rules__"))
            )

    bad_df = df_with_errors.filter(col("__gx_error_rules__") != "").coalesce(1)

    # 원본 DataFrame에서 오류가 있었던 행들을 제외하여 good_df 생성 (left_anti join 사용)
    good_df = df.join(bad_df.select(df.columns), on=df.columns, how="left_anti")

    return good_df, bad_df

def main(args):
    spark = SparkSession.builder.appName(f"GX_Validation_{args.data_asset_name}").getOrCreate()
    df = spark.read.parquet(args.raw_data_path)
    context = get_gx_context(args.s3_bucket_name)
    
    # 로컬 파일 시스템 또는 S3에서 Expectation Suite 로드
    # 현재는 로컬 경로만 가정. context.get_expectation_suite는 로컬 파일시스템을 사용.
    expectation_suite = context.get_expectation_suite(expectation_suite_name=args.suite_name)
    
    # 검증할 데이터가 뭔지 GX에게 알려주는 과정
    validator = context.get_validator(
        batch_request=RuntimeBatchRequest(
            datasource_name="spark_s3_raw_news",
            data_connector_name="runtime_data_connector",
            data_asset_name=args.data_asset_name,
            runtime_parameters={"batch_data": df},
            batch_identifiers={"default_identifier_name": "default_identifier"}
        ),
        expectation_suite=expectation_suite
    )
    validation_result = validator.validate()

    if validation_result["success"]:
        print("All validations passed!")
        df.write.mode("overwrite").parquet(args.passed_data_path)
        sys.exit(0)
    else:
        print("Validations failed. Analyzing results...")
        critical_failure = False
        failed_critical_expectation = None
        non_critical_failures = []

        for result in validation_result["results"]:
            if not result["success"]:
                failure_level = result.get("expectation_config", {}).get("meta", {}).get("failure_level", "Non-critical")
                if failure_level == "Critical":
                    critical_failure = True
                    failed_critical_expectation = result["expectation_config"]["expectation_type"]
                else:
                    non_critical_failures.append(result)

        # Data Docs 경로 생성 (S3 Public URL 형식으로 가정)
        # S3에 생성된 객체의 URL을 얻어오는 더 나은 방법이 필요할 수 있음
        data_docs_path = f"https://{args.s3_bucket_name}.s3.amazonaws.com/gx/data_docs/index.html"

        if critical_failure:
            print(f"CRITICAL FAILURE: {failed_critical_expectation} failed. Stopping pipeline.")
            payload = build_critical_payload(args.data_asset_name, failed_critical_expectation, data_docs_path)
            send_message(args.slack_webhook_url, payload)
            context.build_data_docs()
            sys.exit(1)
        else:
            print(f"{len(non_critical_failures)} non-critical validation(s) failed. Isolating bad data.")
            good_df, bad_df = quarantine_failed_rows(df, non_critical_failures)
            bad_count = bad_df.count()
            good_count = good_df.count()

            payload = build_warning_payload(args.data_asset_name, bad_count, good_count, non_critical_failures, args.quarantine_path, data_docs_path)
            send_message(args.slack_webhook_url, payload)

            print(f"Writing {bad_count} failed rows to {args.quarantine_path}")
            bad_df.write.mode("overwrite").parquet(args.quarantine_path)
            print(f"Writing {good_count} passed rows to {args.passed_data_path}")
            good_df.write.mode("overwrite").parquet(args.passed_data_path)
            
            context.build_data_docs()
            print("Quarantined bad data. Proceeding with good data.")
            sys.exit(0)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket-name", required=True, help="S3 bucket for data and GX artifacts.")
    parser.add_argument("--raw-data-path", required=True, help="S3 path to the raw data parquet file.")
    parser.add_argument("--passed-data-path", required=True, help="S3 path to save data that passed validation.")
    parser.add_argument("--quarantine-path", required=True, help="S3 path to save data that failed validation.")
    parser.add_argument("--expectation-suite-path", required=True, help="Local path to the Expectation Suite JSON file.")
    parser.add_argument("--suite-name", required=True, help="The name of the Expectation Suite.")
    parser.add_argument("--data-asset-name", required=True, help="The name of the data asset being validated.")
    parser.add_argument("--slack-webhook-url", required=True, help="Slack Webhook URL for notifications.")
    
    args = parser.parse_args()
    main(args)
