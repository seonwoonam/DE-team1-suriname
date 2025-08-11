import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional, Sequence, Tuple, Dict, Any
from type.community_crawler import CommunityRequest, CommunityResponse
import aiohttp
from aiohttp import ClientSession, ClientTimeout
import pandas as pd
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type,
)


# -----------------------------
# 설정 및 상수
# -----------------------------


USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
)

LIST_URL_TEMPLATE: str = "https://gall.dcinside.com/board/lists/?id=car_new1&page={page}"
RETRY_STATUS_CODES: set[int] = {429, 500, 502, 503, 504}

# tenacity 관련 설정
MAX_RETRIES: int = 5
JITTER: float = 0.5
BACKOFF_BASE: float = 1.0
BACKOFF_CAP: float = 30.0

# 비동기관련 설정.
DEFAULT_CONCURRENCY: int = 100
# 시간 초과 정책
DEFAULT_TIMEOUTS: ClientTimeout = ClientTimeout(
    total=30, connect=10, sock_read=20, sock_connect=10
)


class RetryableHTTPStatus(Exception):
    """재시도 대상 HTTP 상태 코드를 명시적으로 표현하기 위한 예외."""

    def __init__(self, status: int, url: str) -> None:
        super().__init__(f"Retryable HTTP status {status} for URL: {url}")
        self.status = status
        self.url = url


# -----------------------------
# 크롤러 구현 (비동기)
# -----------------------------

class AsyncDCInsideCrawler:
    """
    DCInside 자동차 갤러리 크롤러의 비동기 구현.

    - 하나의 aiohttp ClientSession을 재사용하여 커넥션 오버헤드 감소.
    - asyncio.Semaphore로 동시 요청 수를 제한.
    - tenacity로 네트워크 오류와 특정 상태 코드(429, 5xx)에 대해 지수 백오프 재시도를 수행.
    """

    def __init__(
        self,
        concurrency: int = DEFAULT_CONCURRENCY,
        timeout: ClientTimeout = DEFAULT_TIMEOUTS,
        headers: Optional[Dict[str, str]] = None,
    ) -> None:
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout = timeout
        self.headers = headers or {"User-Agent": USER_AGENT}

    
    # tenacity가 async def 코루틴을 데코레이팅할 때 대기 구간은 내부적으로
    # await asyncio.sleep(...)을 사용하므로 이벤트 루프를 blocking하지 않는다.
    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_random_exponential(multiplier=BACKOFF_BASE, max=BACKOFF_CAP),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, RetryableHTTPStatus)),
        reraise=True,
    )
    async def request_with_retry(self, session: ClientSession, url: str) -> str:
        async with self.semaphore:
            async with session.get(url, timeout=self.timeout, headers=self.headers) as resp:
                # 상태코드 먼저 확인 가능.
                if resp.status in RETRY_STATUS_CODES:
                    # 재시도 대상 상태 코드인 경우 명시적 예외를 발생시켜 tenacity가 재시도하도록 함
                    raise RetryableHTTPStatus(resp.status, url)
                # 그 외 상태에 대해서는 표준 예외를 발생
                resp.raise_for_status()
                return await resp.text()

    async def _fetch_list_page(self, session: ClientSession, page: int) -> List[str]:
        """
            게시글 링크 수집 함수
        """
        url = LIST_URL_TEMPLATE.format(page=page)
        html = await self.request_with_retry(session, url)
        soup = BeautifulSoup(html, "html.parser")

        links: List[str] = []
        rows = soup.select("tr.ub-content.us-post")
        for row in rows:
            a_tag = row.select_one("td.gall_tit.ub-word a")
            if not a_tag:
                continue
            href = a_tag.get("href")
            if not href:
                continue
            if href.startswith("/board"):
                href = "https://gall.dcinside.com" + href
            links.append(href)
        return links

    async def get_links_from_pages(self, session: ClientSession, start_page: int, end_page: int) -> List[str]:
        """
            게시글 링크 수집 요청 함수
        """
        tasks = [asyncio.create_task(self._fetch_list_page(session, p)) for p in range(start_page, end_page + 1)]
        # return_exceptions=True 옵션을 사용하면 예외가 발생하더라도 계속 진행.
        
        ##############
        # as_complete로 바꿔줘야 할듯.
        results = await asyncio.gather(*tasks, return_exceptions=True)

        collected: List[str] = []
        for idx, result in enumerate(results, start=start_page):
            if isinstance(result, Exception):
                logging.warning("Failed to fetch list page %s: %s", idx, result)
                continue
            collected.extend(result)

        # 중복 제거(원래 순서 보존)
        seen: set[str] = set()
        unique_links: List[str] = []
        for link in collected:
            if link not in seen:
                seen.add(link)
                unique_links.append(link)
        return unique_links


    def _extract_post_body(self, soup: BeautifulSoup) -> str:
        """
            게시글 본문 추출 함수
        """
        write_div = soup.select_one("div.write_div")
        if write_div is None:
            return " "

        # 특정 클래스 요소 제거 후 텍스트만 추출
        excluded_classes = ["imgwrap", "og-div"]
        for excluded_class in excluded_classes:
            for element in write_div.find_all(class_=excluded_class):
                element.extract()

        body_text = write_div.get_text(separator="\n", strip=True)
        if not body_text:
            return " "

        suffix = "- dc official App"
        if body_text.endswith(suffix):
            body_text = body_text[: -len(suffix)].strip()
        return body_text

    def _extract_up_down(self, soup: BeautifulSoup) -> Tuple[int, int]:
        """
            게시글 추천 수 추출 함수
        """
        def safe_int(text: Optional[str]) -> int:
            if not text:
                return 0
            try:
                return int(text.replace(",", "").strip())
            except Exception:
                return 0

        up_text = None
        down_text = None

        up_node = soup.select_one("div.up_num_box p.up_num")
        if up_node is not None:
            up_text = up_node.get_text()

        down_node = soup.select_one("div.down_num_box p.down_num")
        if down_node is not None:
            down_text = down_node.get_text()

        return safe_int(up_text), safe_int(down_text)


    async def parse_post(self, session: ClientSession, url: str) -> Optional[CommunityResponse]:
        """
            게시글 상세 페이지 전체 파싱
            - 게시글 상세 내용 요청 및 파싱 함수.
        """
        try:
            html = await self.request_with_retry(session, url)
            soup = BeautifulSoup(html, "html.parser")

            # 게시 시각
            date_element = soup.select_one("div.fl span.gall_date")
            if date_element and date_element.text:
                try:
                    date_str, time_str = date_element.text.split()
                    post_time = datetime.strptime(f"{date_str} {time_str}", "%Y.%m.%d %H:%M:%S")
                except Exception:
                    post_time = datetime.utcnow()
            else:
                post_time = datetime.utcnow()

            # 제목
            title_element = soup.select_one("h3.title.ub-word span.title_subject")
            title = title_element.get_text(strip=True) if title_element else "Untitled"

            # 조회수 (보수적으로 파싱)
            view_count: int = 0
            try:
                fr_blocks = soup.select("div.fr")
                if len(fr_blocks) > 1:
                    parts = fr_blocks[1].get_text(strip=True).split()
                    # 기대 포맷이 다를 수 있으므로 숫자만 추출
                    for token in parts:
                        if token.replace(",", "").isdigit():
                            view_count = int(token.replace(",", ""))
                            break
            except Exception:
                view_count = 0

            # 본문
            content = self._extract_post_body(soup)
            if not content:
                content = " "

            # 추천 수
            like_count, _ = self._extract_up_down(soup)

            return CommunityResponse(
                post_time=post_time,
                title=title,
                content=content,
                view_count=view_count,
                like_count=like_count,
                source="dcinside",
                link=url,
            )
        except Exception as exc:
            logging.warning("Failed to parse post %s: %s", url, exc)
            return None


    async def crawl_posts(self, session: ClientSession, links: Sequence[str]) -> List[CommunityResponse]:
        """
            크롤링 시작 함수
        """
        tasks = [asyncio.create_task(self.parse_post(session, link)) for link in links]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        items: List[CommunityResponse] = []
        for link, result in zip(links, results):
            if isinstance(result, Exception):
                logging.warning("Exception while crawling %s: %s", link, result)
                continue
            if result is None:
                continue
            items.append(result)
        return items


# -----------------------------
# 유틸리티
# -----------------------------

def load_links_from_csv(csv_path: str) -> List[str]:
    df = pd.read_csv(csv_path)
    if "link" not in df.columns:
        raise ValueError("CSV must contain a 'link' column")
    return df["link"].dropna().astype(str).tolist()


def save_posts_to_parquet(items: Iterable[CommunityResponse], output_path: str) -> None:
    df = pd.DataFrame([item.to_dict() for item in items])
    df.to_parquet(output_path, engine="pyarrow")

def save_links_to_csv(links: Sequence[str], csv_path: str) -> None:
    pd.DataFrame({"link": list(links)}).to_csv(csv_path, index=False)


# -----------------------------
# 실행 진입점
# -----------------------------

async def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Async DCInside car gallery crawler")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--from-pages",
        nargs=2,
        metavar=("START_PAGE", "END_PAGE"),
        type=int,
        help="목록 페이지 구간에서 링크를 수집하여 크롤링합니다.",
    )
    group.add_argument(
        "--from-csv",
        metavar="CSV_PATH",
        type=str,
        help="사전 수집된 links.csv에서 링크를 불러와 크롤링합니다.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="dcinside_posts.parquet",
        help="결과 Parquet 파일 경로",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help="동시 요청 수 제한",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    parser.add_argument(
        "--links-csv",
        type=str,
        default="dcinside_links.csv",
        help="수집한 링크를 저장할 CSV 파일 경로 (--from-pages 사용 시 유효)",
    )

    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))

    crawler = AsyncDCInsideCrawler(concurrency=args.concurrency)
    async with aiohttp.ClientSession(timeout=DEFAULT_TIMEOUTS) as session:
        if args.from_pages:
            start_page, end_page = args.from_pages
            # 의존성 주입. 전역 변수 link에 의존하던 것 수정.
            links = await crawler.get_links_from_pages(session, start_page, end_page)
            try:
                save_links_to_csv(links, args.links_csv)
                logging.info("Saved %d links to %s", len(links), args.links_csv)
            except Exception as exc:
                logging.warning("Failed to save links CSV: %s", exc)
        else:
            links = load_links_from_csv(args.from_csv)

        if not links:
            logging.warning("No links to crawl. Exiting.")
            return

        items = await crawler.crawl_posts(session, links)
        if not items:
            logging.warning("No items crawled. Exiting.")
            return

        save_posts_to_parquet(items, args.output)
        logging.info("Saved %d posts to %s", len(items), args.output)


if __name__ == "__main__":
    asyncio.run(main())

"""
 실행방법
    cd extract/community/crawler
    python3 dcinside_crawler_async.py --from-pages 12228 12785 --output dcinside_posts.parquet --concurrency 100
"""