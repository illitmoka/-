import asyncio
from datetime import datetime, timedelta, timezone
import pickle
import hashlib
import dc_api

def generate_unique_id(doc):
    """
    주어진 문서(doc)에 대해 고유 식별자를 생성합니다.
    - doc.id가 존재하면 이를 문자열로 반환합니다.
    - 그렇지 않으면, doc.author, doc.time, doc.author_id와 선택적으로
      doc.content, doc.title 등의 속성을 조합하여 SHA256 해시를 생성합니다.
    """
    if hasattr(doc, 'id') and doc.id is not None:
        return str(doc.id)
    else:
        base_str = f"{doc.author}_{doc.time}_{doc.author_id}"
        if hasattr(doc, 'content') and doc.content:
            base_str += f"_{doc.content}"
        if hasattr(doc, 'title') and doc.title:
            base_str += f"_{doc.title}"
        return hashlib.sha256(base_str.encode('utf-8')).hexdigest()

async def safe_get_document(index, retries=10, delay=0.2):
    """
    문서를 가져올 때 최대 retries 횟수만큼 재시도합니다.
    재시도 시 지수 백오프를 적용하여 호출 간 간격을 늘립니다.
    """
    for attempt in range(retries):
        try:
            doc = await index.document()
            if doc is not None:
                return doc
        except Exception as e:
            print(f"[게시글 집계 재시도 {attempt+1}/{retries}] 문서 가져오기 오류: {e}")
        await asyncio.sleep(delay * (2 ** attempt))
    print("[안전 재시도 실패] 문서 가져오기에 실패하였습니다.")
    return None

async def safe_iterate_comments(index, retries=10, delay=0.2):
    """
    댓글을 가져올 때 최대 retries 횟수만큼 재시도합니다.
    재시도 시 지수 백오프를 적용하여 호출 간 간격을 늘립니다.
    """
    for attempt in range(retries):
        try:
            async for comm in index.comments():
                yield comm
            return  # 정상적으로 모든 댓글을 순회했으면 함수 종료
        except Exception as e:
            print(f"[댓글 집계 재시도 {attempt+1}/{retries}] 댓글 가져오기 오류: {e}")
            await asyncio.sleep(delay * (2 ** attempt))
    print("[안전 재시도 실패] 댓글 가져오기에 실패하였습니다.")
    # 실패 시 더 이상 댓글을 처리하지 않습니다.

def get_doc_time_utc(doc_time):
    """
    doc_time이 naive하면 UTC로 간주하고, tzinfo가 없으면 강제로 UTC로 설정합니다.
    tzinfo가 있는 경우에는 UTC로 변환합니다.
    """
    if doc_time.tzinfo is None:
        return doc_time.replace(tzinfo=timezone.utc)
    else:
        return doc_time.astimezone(timezone.utc)

async def process_post(index, rank, data, processed_article_ids):
    """
    하나의 게시글(문서)을 처리합니다.
    처리 결과에 따라 아래 세 가지 상태값을 반환합니다.
      - "in_range": 지정한 범위 내의 게시글 (정상 처리)
      - "skip": 중복이거나, 아직 처리 대상이 아닌 게시글 (예: 미래의 글)
      - "old": 지정 범위 이전의 게시글 (범위 외)
    """
    # 1. 문서 가져오기 (안전한 재시도 적용)
    doc = await safe_get_document(index)
    if doc is None:
        return "skip"  # 문서를 가져오지 못하면 건너뜁니다.
    
    # 2. 고유 식별자 생성 및 중복 체크
    article_uid = generate_unique_id(doc)
    if article_uid in processed_article_ids:
        print(f"중복 글 발견: {article_uid} 집계 제외")
        return "skip"
    processed_article_ids.add(article_uid)
    
    # 3. 집계 대상 기간 설정 (2025년 1월 1일 ~ 2025년 1월 31일 UTC)
    target_start = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    target_end = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    try:
        doc_time = get_doc_time_utc(doc.time)
    except Exception as e:
        print(f"[시간 파싱 오류] {e}")
        return "skip"
    
    # 미래의 글은 아직 집계 대상이 아니므로 건너뜁니다.
    if doc_time > target_end:
        return "skip"
    # 지정한 범위 이전의 글은 범위 외 상태로 반환
    if doc_time < target_start:
        print(f"[범위 외] {doc_time}은(는) 지정한 집계 대상 범위({target_start} 이상)보다 이전입니다.")
        return "old"
    
    # 4. 전역 게시글 카운트 업데이트 및 중간 저장 (10번째마다)
    data['global_count'] = data.get('global_count', 0) + 1
    if data['global_count'] % 10 == 0:
        elapsed_minutes = (datetime.now(timezone.utc) - data['start_time']).total_seconds() / 60.0
        print(f"[중간 저장] {doc.time} | 집계 시간 경과: {elapsed_minutes:.2f} 분")
        data['rank'] = rank
        with open('data.pickle', 'wb') as fw:
            pickle.dump(data, fw)
    
    # 5. 집계 기간 업데이트
    if data['start_date'] is None or doc_time < data['start_date']:
        data['start_date'] = doc_time
    if data['end_date'] is None or doc_time > data['end_date']:
        data['end_date'] = doc_time
    
    # 6. 게시글 작성자 집계
    try:
        writer = doc.author if doc.author_id is None else f"{doc.author}({doc.author_id})"
        if writer in rank:
            rank[writer]['article'] += 1
        else:
            rank[writer] = {"article": 1, "reply": 0}
    except Exception as e:
        print(f"[게시글 집계 오류] {e}")
    
    # 7. 댓글 집계 (재시도 및 중복 방지 적용)
    try:
        processed_comment_ids = set()
        async for comm in safe_iterate_comments(index):
            try:
                # 댓글의 고유 식별자 생성
                if hasattr(comm, 'id') and comm.id is not None:
                    unique_id = comm.id
                else:
                    unique_id = (comm.author, getattr(comm, 'content', None), getattr(comm, 'time', None))
                if unique_id in processed_comment_ids:
                    continue
                processed_comment_ids.add(unique_id)
                
                writer = comm.author if comm.author_id is None else f"{comm.author}({comm.author_id})"
                if writer in rank:
                    rank[writer]['reply'] += 1
                else:
                    rank[writer] = {"article": 0, "reply": 1}
            except Exception as e:
                print(f"[댓글 집계 오류] {e}")
    except Exception as e:
        print(f"[댓글 처리 최종 오류] {e}")
        await asyncio.sleep(0.01)
    
    return "in_range"

async def run():
    """
    메인 함수:
    - dc_api를 통해 지정된 게시판의 게시글들을 순회하며 처리합니다.
    - 게시글 및 댓글 집계를 진행하고, 집계 기간 업데이트와 중간 저장을 수행합니다.
    - 최종 결과를 pickle 파일에 저장합니다.
    
    개선점:
    - 범위 외 게시글("old")이 나타나더라도 바로 크롤링을 중단하지 않고,
      연속해서 범위 외 게시글이 일정 횟수 이상 나오면 중단하도록 합니다.
    """
    data = {
        'start_date': None,
        'end_date': None,
        'date': datetime.now(timezone.utc),
        'global_count': 0,
        'start_time': datetime.now(timezone.utc)
    }
    rank = {}
    processed_article_ids = set()
    consecutive_old_count = 0   # 연속 범위 외 게시글 개수를 카운트합니다.
    OLD_THRESHOLD = 20          # 연속 10개 이상이면 중단
    
    async with dc_api.API() as api:
        async for index in api.board(board_id="mokalatte", is_minor=True, start_page=87):
            status = await process_post(index, rank, data, processed_article_ids)
            if status == "old":
                consecutive_old_count += 1
                if consecutive_old_count >= OLD_THRESHOLD:
                    print(f"연속 {OLD_THRESHOLD}개 이상의 범위 외 게시글이 발견되어 크롤링을 중단합니다.")
                    break
            else:
                # in_range 또는 skip이면 연속 범위 외 카운트를 리셋합니다.
                consecutive_old_count = 0

    if data['start_date'] is None:
        print("유효한 게시글이 없습니다. fallback 집계 기간 사용")
        data['start_date'] = data['date'] - timedelta(days=1)
        data['end_date'] = data['date']

    data['rank'] = rank
    with open('data.pickle', 'wb') as fw:
        pickle.dump(data, fw)

    print("최종 집계 날짜")
    print(f"start_date: {data['start_date']}")
    print(f"end_date:   {data['end_date']}")
    print(data)
    input("종료하려면 Enter 키를 누르세요.")

if __name__ == '__main__':
    asyncio.run(run())
