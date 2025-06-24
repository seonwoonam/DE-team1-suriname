import json
import re
import random
import time
from datetime import datetime
from typing import Optional, List

import requests
from bs4 import BeautifulSoup

from base_crawler import BaseCrawler
from type.news_crawler import NewsRequest, NewsResponse

MAX_RETRIES = 5         
BACKOFF_BASE = 1.0        
BACKOFF_CAP = 30.0     
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

class YNACrawler(BaseCrawler):
    def __init__(self, request : NewsRequest):
        super().__init__(request, source='YNA')
        self.page_base_url = "http://ars.yna.co.kr/api/v2/search.asis?callback=Search.SearchPreCallback&ctype=A&page_size=10&channel=basic_kr"
        self.news_base_url = "https://www.yna.co.kr/view/"

    def request_with_backoff(self,param:Optional[dict], url:str) -> Optional[requests.Response]:
        attempt = 0
        while attempt < MAX_RETRIES:
            try :
                res = requests.get(url, params=param, headers= self.headers)
                if res.status_code < 400 or res.status_code not in RETRY_STATUS_CODES:
                    return res
            except requests.RequestException as exc:
                res = None 
            
            if attempt == MAX_RETRIES:
                return res 
            sleep = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** attempt))
            # jitter도 적용
            delay = random.uniform(BACKOFF_BASE *(2 ** attempt-1), sleep) 
            print(f"[retry] attempt={attempt+1} sleeping {delay:.2f}s before retry …")
            time.sleep(delay)
            attempt += 1
        return None

    def get_search_result(self)-> Optional[List[NewsResponse]]:
        news_input = self.request
        news_output_total = []
        page_no = 1
        print(self.request['keyword'])
        while True:
            news_list = self.get_page_content(news_input, page_no)
            if len(news_list) != 0:
                news_output = self.get_news_content(news_list)
                if len(news_output) != 0:
                    print(f'{len(news_output)} news pages extracted')
                    news_output_total.extend(news_output)
            else:
                break
            page_no += 1
        
        return news_output_total

    def get_page_content(self, news_input: NewsRequest, page_no: int) -> List[dict]:
        news_list = []
        params = {
            'query': news_input['keyword'],
            'page_no': page_no,
            'period': 'diy',
            'from': news_input['start_time'].strftime('%Y%m%d'),
            'to': news_input['end_time'].strftime('%Y%m%d')
        }

        response = self.request_with_backoff(params,self.page_base_url)
        if response.status_code == 200:
            response_json = self._response_parser(response)
            news_list = response_json['KR_ARTICLE']['result']

        return news_list
    
    def get_news_content(self, news_list: List[dict]) -> List[NewsResponse]:
        news_output:List[NewsResponse] = []
        for news in news_list:
            news_title = news['TITLE']
            news_url = f'{self.news_base_url}{news["CONTENTS_ID"]}'
            news_response = self.request_with_backoff(url=news_url)
            if news_response.status_code == 200:
                news_soup = BeautifulSoup(news_response.text, 'html.parser')
                news_date_str = news_soup.select_one('.update-time')["data-published-time"]
                news_date = datetime.strptime(news_date_str, '%Y-%m-%d %H:%M')
                if self.request['start_time'] <= news_date < self.request['end_time']:
                    news_body_list = news_soup.select('article.story-news.article > p:not([class])')
                    news_body = " ".join(p.text.strip() for p in news_body_list)

                    news_output.append(
                        NewsResponse(
                            post_time=news_date,
                            title=news_title,
                            content=news_body,
                            source=self.source,
                            link=news_url,
                            keyword=self.request['keyword']
                        )
                    )
        return news_output

    def _response_parser(self, response: requests.Response) -> json:
        response_json_str = re.sub(r"^Search\.SearchPreCallback\(|\)$", "", response.text)
        return json.loads(response_json_str)