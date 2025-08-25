from typing import Any, TypedDict, List, Dict
from datetime import datetime
from dataclasses import dataclass, asdict

# 커뮤니티 댓글 타입 (키 : 내용)
class CommunityComment(TypedDict):
    content : str

# 입력 데이터의 타입 (키: 시작시간, 끝시간, 키워드)
class CommunityRequest(TypedDict):
    start_time : datetime
    end_time : datetime

# 출력 데이터의 타입 (키: 게시날짜, 제목, 본문, 댓글, 조회수, 좋아요, 출처, 링크)
@dataclass
class CommunityResponse:
    post_time : datetime
    title : str
    content : str
    view_count : int
    like_count : int
    source : str
    link : str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)