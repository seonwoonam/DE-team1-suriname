from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator ## RDS 조회 후 community_dag 실행 여부를 결정
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from conf import (
    EMR_EXECUTION_ROLE_ARN,
    EMR_APPLICATION_ID,
    GX_PROJECT_ARCHIVE,
    S3_BUCKET_NAME,
    S3_NEWS_DATA,
    S3_NEWS_PASSED_DATA,
    S3_NEWS_QUARANTINE_DATA,
    S3_NEWS_OUTPUT,
    GX_VALIDATION_ENTRY_POINT,
    GX_NEWS_SUITE_PATH,
    NEWS_ENTRY_POINT,
    DEV_WEBHOOK_URL,
)
from datetime import datetime, timedelta
import json
import base64

test_start_time = Variable.get("TEST_START_TIME")
test_end_time = Variable.get("TEST_END_TIME")

def send_slack_alert_callback(context):

    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url

    slack_data = {
        "webhook_url" : DEV_WEBHOOK_URL,
        "payload" : {
            "dag_id": dag_id,
            "task_id" : task_id,
            "execution_date": execution_date,
            "log_url" : log_url,
        }
    }

    json_payload = json.dumps(slack_data, default=str)

    
    # LambdaInvokeFunctionOperator 인스턴스를 생성하고 실행
    operator = LambdaInvokeFunctionOperator(
        task_id='send_slack_alert_for_developer',
        function_name='lambda_slack_alert_for_developer',
        payload=json_payload,
        aws_conn_id=None,
        region_name='ap-northeast-2',
        execution_timeout=timedelta(minutes=5)
    )
    operator.execute(context=context)

# ✅ "YYYY-MM-DDTHH:MM" -> "YYYY-MM-DD-HH-MM-SS" 변환 함수
def format_time_variable(time_str):
    return time_str.replace("T", "-").replace(":", "-") + "-00"

# ✅ 변환된 값
formatted_start_time = format_time_variable(test_start_time)
formatted_end_time = format_time_variable(test_end_time)

# ✅ test_batch_period 만들기
test_batch_period = f"{formatted_start_time}_{formatted_end_time}"

# 기본 설정 (Owner, 시작 날짜, 재시도 설정)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'on_failure_callback': send_slack_alert_callback
}

# DAG 정의
dag = DAG(
    'news_dag',
    default_args=default_args,
    # schedule_interval=timedelta(hours=24),  # 수정된 스케줄
    schedule_interval=None,  # test
    catchup=False  # 과거 데이터 재실행 안 함
)

# RDS에서 is_issue가 True인 데이터 조회하는 Python 함수
def check_rds_issue(**kwargs):
    hook = PostgresHook(postgres_conn_id="rds_default")  # MWAA Connection ID 사용
    conn = hook.get_conn()
    cursor = conn.cursor()

    query = """
    SELECT car_model, accident FROM accumulated_table WHERE is_issue = TRUE;
    """
    cursor.execute(query)
    result = cursor.fetchall()

    cursor.close()
    conn.close()

    # 결과가 있다면 XCom에 저장 (community_dag에 넘길 데이터)
    if result:
        issue_list = [{"car_model": row[0], "accident": row[1]} for row in result]
        kwargs['ti'].xcom_push(key='issue_list', value=issue_list)
        return "trigger_community_dag"  # community_dag 실행 조건 만족
    else:
        return "skip_community_dag"  # 실행 조건 불충족


# 언론사 리스트
news_sources = ["ytn", "sbs", "kbs", "yna"]

# Extract Lambda 호출 Task 리스트
extract_lambda_tasks = []

for source in news_sources:
    lambda_task = LambdaInvokeFunctionOperator(
        task_id=f'invoke_news_extract_{source}',
        function_name='news_extract_function',
        payload=json.dumps({
            "source": source,
            # 한국 시간(KST)으로 변환
            # "start_time_str": "{{ (data_interval_start + macros.timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M') }}",
            # "end_time_str": "{{ (data_interval_end + macros.timedelta(hours=9)).strftime('%Y-%m-%dT%H:%M') }}"
            "start_time_str": test_start_time, # test
            "end_time_str": test_end_time # test
        }),
        aws_conn_id=None, # MWAA에서는 필요 없음
        region_name='ap-northeast-2',
        execution_timeout=timedelta(minutes=5),  # 5분 제한
        dag=dag  # DAG 명시적으로 추가
    )
    extract_lambda_tasks.append(lambda_task)



s3_news_data = S3_NEWS_DATA
s3_news_passed_data = S3_NEWS_PASSED_DATA
s3_news_quarantine_data = S3_NEWS_QUARANTINE_DATA
s3_news_output = S3_NEWS_OUTPUT
accident_keyword_original = Variable.get("ACCIDENT_KEYWORD")
encoded_value = base64.b64encode(json.dumps(accident_keyword_original, ensure_ascii=False).encode('utf-8')).decode('utf-8')
Variable.set("ACCIDENT_KEYWORD_ENCODED", encoded_value)
accident_keyword = Variable.get("ACCIDENT_KEYWORD_ENCODED")
gpt = Variable.get("GPT")

# =================================================================================================
# ================== 1. 데이터 품질 검증(Great Expectations) EMR Serverless 실행 Task ==================
# =================================================================================================
validate_entryPointArguments = [
    "--s3-bucket-name", S3_BUCKET_NAME,
    "--raw-data-path", f"{s3_news_data}{test_batch_period}/", # 원본 데이터 경로
    "--passed-data-path", f"{s3_news_passed_data}{test_batch_period}/", # 통과 데이터 저장 경로
    "--quarantine-path", f"{s3_news_quarantine_data}{test_batch_period}/", # 격리 데이터 저장 경로
    "--expectation-suite-path", GX_NEWS_SUITE_PATH, # EMR에 배포된 경로
    "--suite-name", "news_raw_data_suite",
    "--data-asset-name", "NewsRawData",
    "--slack-webhook-url", DEV_WEBHOOK_URL
]

validate_news_data_quality = EmrServerlessStartJobOperator(
    task_id='validate_news_data_quality',
    application_id=EMR_APPLICATION_ID,
    execution_role_arn=EMR_EXECUTION_ROLE_ARN,
    job_driver={
        "sparkSubmit": {
            "entryPoint": GX_VALIDATION_ENTRY_POINT, # S3에 저장된 run_gx_validation.py
            "entryPointArguments": validate_entryPointArguments,
            "sparkSubmitParameters": f"--conf spark.archives={GX_PROJECT_ARCHIVE}" # GX 프로젝트 압축 파일
        }
    },
    configuration_overrides={},
    aws_conn_id=None,
    dag=dag

)

# =================================================================================================
# ================== 2. 데이터 변환(Transform) EMR Serverless 실행 Task =============================
# =================================================================================================
transform_entryPointArguments = [
    "--data_source", f"{s3_news_passed_data}{test_batch_period}/", # 검증을 통과한 데이터를 입력으로 사용
    "--output_uri", s3_news_output,
    "--batch_period", test_batch_period,
    "--accident_keyword", accident_keyword,
    "--gpt", gpt
]

emr_serverless_task = EmrServerlessStartJobOperator(
    task_id='run_news_emr_transform',
    application_id=EMR_APPLICATION_ID,
    execution_role_arn=EMR_EXECUTION_ROLE_ARN,
    job_driver={
        "sparkSubmit": {
            "entryPoint": NEWS_ENTRY_POINT,
            "entryPointArguments": transform_entryPointArguments,
            "sparkSubmitParameters": "--conf spark.executor.memory=4g --conf spark.driver.memory=2g"
        }
    },
    configuration_overrides={},
    aws_conn_id=None,
    dag=dag
)

# =================================================================================================
# ================== 3. 데이터 적재(Load) Lambda 실행 Task ========================================
# =================================================================================================
lambda_load_news_task = LambdaInvokeFunctionOperator(
    task_id='invoke_lambda_load_news',
    function_name='lambda_load_news',
    payload=json.dumps({
        "batch_period": test_batch_period,
        "threshold": Variable.get("THRESHOLD", 10),
        "dbname": Variable.get("RDS_DBNAME"),
        "user": Variable.get("RDS_USER"),
        "password": Variable.get("RDS_PASSWORD"),
        "url": Variable.get("RDS_HOST"),
        "port": Variable.get("RDS_PORT"),
        "bucket_name": S3_BUCKET_NAME
    }),
    aws_conn_id=None,
    region_name='ap-northeast-2',
    execution_timeout=timedelta(minutes=5),
    dag=dag
)

# =================================================================================================
# ================== 4. 후속 DAG 트리거 로직 ======================================================
# =================================================================================================
check_issue_task = BranchPythonOperator(
    task_id='check_rds_issue',
    python_callable=check_rds_issue,
    provide_context=True,
    dag=dag
)

trigger_community_dag = TriggerDagRunOperator(
    task_id='trigger_community_dag',
    trigger_dag_id='community_dag',
    conf={"issue_list": "{{ ti.xcom_pull(task_ids='check_rds_issue', key='issue_list') }}",
          "data_interval_start": "{{ data_interval_start }}",
          "data_interval_end": "{{ data_interval_end }}"},
    dag=dag
)

skip_community_dag = DummyOperator(
    task_id="skip_community_dag",
    dag=dag
)

# =================================================================================================
# ================== 최종 Task 의존성 설정 ========================================================
# =================================================================================================
extract_lambda_tasks >> validate_news_data_quality >> emr_serverless_task >> lambda_load_news_task >> check_issue_task >> [trigger_community_dag, skip_community_dag]
