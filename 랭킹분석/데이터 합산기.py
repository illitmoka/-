import re
from datetime import datetime
from bs4 import BeautifulSoup
from functools import reduce
from concurrent.futures import ThreadPoolExecutor

def parse_report_html(file_path: str) -> dict:
    """
    HTML 보고서 파일을 읽어 집계 기간과 사용자 데이터를 파싱합니다.
    사용자 데이터는 user_id를 키로 하며, 글/댓글 수와 닉네임 집합(nicks)을 저장합니다.
    """
    # 파일 경로가 빈 문자열이면 경고를 출력하고 빈 report 반환
    if not file_path.strip():
        print("경고: 파일 경로가 비어 있습니다. 해당 파일은 건너뛰겠습니다.")
        return {"start_date": "", "end_date": "", "users": {}}
    
    try:
        with open(file_path, encoding='utf-8') as f:
            html = f.read()
    except FileNotFoundError:
        print(f"경고: 파일을 찾을 수 없습니다: {file_path}")
        return {"start_date": "", "end_date": "", "users": {}}
    
    soup = BeautifulSoup(html, 'html.parser')
    
    # 집계 기간 파싱 (예: "집계 기간 : 2025/02/01 00:00:00 ~ 2025/02/08 00:00:00")
    period_tag = soup.find('p', string=re.compile("집계 기간"))
    if period_tag:
        period_text = period_tag.get_text()
        # 날짜와 시간 사이의 공백을 포함하도록 정규표현식을 수정
        m = re.search(
            r"집계 기간\s*:\s*(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\s*~\s*(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})",
            period_text
        )
        if m:
            start_date_str, end_date_str = m.groups()
        else:
            start_date_str, end_date_str = "", ""
    else:
        start_date_str, end_date_str = "", ""
    
    # 사용자 정보 파싱
    # 각 사용자의 정보는 "<p>숫자등 : 닉네임1,닉네임2,...(user_id) | 총 점: ... | 글 X개(...), 댓글 Y개(...)</p>" 형식입니다.
    user_tags = soup.find_all('p', string=re.compile(r"\d+등\s*:"))
    users = {}
    for tag in user_tags:
        text = tag.get_text().strip()
        # 정규표현식으로 닉네임 문자열, user_id, 글 개수, 댓글 개수를 추출합니다.
        m = re.search(r"\d+등\s*:\s*(.*?)\((.*?)\).*?글\s*(\d+)개.*?댓글\s*(\d+)개", text)
        if m:
            nick_str, user_id, article_str, reply_str = m.groups()
            article = int(article_str)
            reply = int(reply_str)
            # 쉼표로 구분된 닉네임들을 분리하고 앞뒤 공백 제거 후 중복 제거
            nick_list = [n.strip() for n in nick_str.split(",") if n.strip()]
            if user_id in users:
                users[user_id]["article"] += article
                users[user_id]["reply"] += reply
                users[user_id]["nicks"].update(nick_list)
            else:
                users[user_id] = {"article": article, "reply": reply, "nicks": set(nick_list)}
    return {"start_date": start_date_str, "end_date": end_date_str, "users": users}

def merge_reports(report1: dict, report2: dict) -> dict:
    """
    두 개의 보고서 데이터를 병합합니다.
    - 집계 기간은 두 보고서의 시작/종료 시각을 각각 최솟값/최댓값으로 설정합니다.
    - 사용자 데이터는 동일 user_id 기준으로 글/댓글 수를 합산하고 닉네임 집합을 업데이트합니다.
    """
    fmt = '%Y/%m/%d %H:%M:%S'
    try:
        start_date1 = datetime.strptime(report1["start_date"], fmt)
    except:
        start_date1 = None
    try:
        start_date2 = datetime.strptime(report2["start_date"], fmt)
    except:
        start_date2 = None
    try:
        end_date1 = datetime.strptime(report1["end_date"], fmt)
    except:
        end_date1 = None
    try:
        end_date2 = datetime.strptime(report2["end_date"], fmt)
    except:
        end_date2 = None

    if start_date1 and start_date2:
        combined_start = min(start_date1, start_date2)
    else:
        combined_start = start_date1 or start_date2
    if end_date1 and end_date2:
        combined_end = max(end_date1, end_date2)
    else:
        combined_end = end_date1 or end_date2

    merged_users = {}
    for report in (report1, report2):
        for user_id, data in report["users"].items():
            if user_id in merged_users:
                merged_users[user_id]["article"] += data["article"]
                merged_users[user_id]["reply"] += data["reply"]
                merged_users[user_id]["nicks"].update(data["nicks"])
            else:
                merged_users[user_id] = {
                    "article": data["article"],
                    "reply": data["reply"],
                    "nicks": set(data["nicks"])
                }
    return {
        "start_date": combined_start.strftime(fmt) if combined_start else "",
        "end_date": combined_end.strftime(fmt) if combined_end else "",
        "users": merged_users
    }

def compute_article_rank(users: dict) -> None:
    """
    글 순위를 계산하여 각 사용자 데이터에 articleRank를 추가합니다.
    """
    sorted_by_article = sorted(users.items(), key=lambda x: x[1]["article"], reverse=True)
    rank = 1
    for i, (uid, d) in enumerate(sorted_by_article):
        if i > 0 and sorted_by_article[i-1][1]["article"] != d["article"]:
            rank += 1
        users[uid]["articleRank"] = rank

def compute_reply_rank(users: dict) -> None:
    """
    댓글 순위를 계산하여 각 사용자 데이터에 replyRank를 추가합니다.
    """
    sorted_by_reply = sorted(users.items(), key=lambda x: x[1]["reply"], reverse=True)
    rank = 1
    for i, (uid, d) in enumerate(sorted_by_reply):
        if i > 0 and sorted_by_reply[i-1][1]["reply"] != d["reply"]:
            rank += 1
        users[uid]["replyRank"] = rank

def calculate_scores_and_ranks(users: dict) -> None:
    """
    사용자 데이터에 대해 총 점수(글 3점, 댓글 1점)를 계산하고,
    글 및 댓글 수에 따른 순위를 각각 계산하여 저장합니다.
    """
    # 총 점수 계산
    for user_id, data in users.items():
        data["score"] = data["article"] * 3 + data["reply"]

    # 글 순위와 댓글 순위를 병렬로 계산
    with ThreadPoolExecutor() as executor:
        future_article = executor.submit(compute_article_rank, users)
        future_reply = executor.submit(compute_reply_rank, users)
        future_article.result()
        future_reply.result()

def extract_all_users(users: dict) -> (list, float):
    """
    총 점수를 기준으로 내림차순 정렬한 사용자 리스트와 전체 총 점수를 반환합니다.
    """
    sorted_by_score = sorted(users.items(), key=lambda x: x[1]["score"], reverse=True)
    all_users = []
    rank = 1
    for i, (uid, d) in enumerate(sorted_by_score):
        if i > 0 and sorted_by_score[i-1][1]["score"] != d["score"]:
            rank += 1
        all_users.append((rank, uid, d))
    total_score = sum(d["score"] for uid, d in sorted_by_score)
    return all_users, total_score

def generate_html_report(start_date: str, end_date: str, users: dict, all_users: list, total_score: float) -> str:
    """
    최종 HTML 보고서를 생성합니다.
    """
    total_articles = sum(d["article"] for d in users.values())
    total_replies = sum(d["reply"] for d in users.values())
    total_users = len(users)  # 전체 사용자 수 계산
    
    lines = [
        "<html>",
        "<head>",
        '    <meta charset="UTF-8">',
        "    <title>합산 집계 결과</title>",
        "</head>",
        '<body style="font-size:12px; font-family: sans-serif;">',
        f"    <p>집계 기간 : {start_date} ~ {end_date}</p>",
        "    <p>(글+3점, 댓글+1점)</p>",
        f"    <p>총 글 수: {total_articles}개, 총 댓글 수: {total_replies}개</p>",
        f"    <p>총 갤러 수: {total_users}명</p>"
    ]
    
    for rank_val, uid, d in all_users:
        # 정렬된 닉네임 집합은 중복 없이 하나씩만 출력됩니다.
        nicks = ",".join(sorted(d["nicks"]))
        share = (d["score"] / total_score * 100) if total_score > 0 else 0
        line = (f"    <p>{rank_val}등 : {nicks}({uid}) | 총 점: {d['score']}점 | "
                f"갤 지분(총점): {share:.2f}% | 글 {d['article']}개({d['articleRank']}위), "
                f"댓글 {d['reply']}개({d['replyRank']}위)</p>")
        lines.append(line)
        lines.append("    <hr>")
    
    lines.extend(["</body>", "</html>"])
    return "\n".join(lines)

def merge_reports_from_multiple_files(file_list: list) -> str:
    """
    여러 HTML 보고서 파일을 읽어 파싱한 후 모두 병합하고, 최종 합산 HTML 보고서를 반환합니다.
    """
    # 빈 파일 경로는 필터링
    valid_files = [file for file in file_list if file.strip()]
    if not valid_files:
        print("유효한 파일 경로가 하나도 없습니다.")
        return ""
    
    # 파일 읽기 및 파싱을 병렬 처리하여 I/O 병목 개선
    with ThreadPoolExecutor() as executor:
        reports = list(executor.map(parse_report_html, valid_files))
    
    # reduce를 사용하여 모든 보고서를 순차적으로 병합합니다.
    merged_report = reduce(merge_reports, reports)
    users = merged_report["users"]
    calculate_scores_and_ranks(users)
    all_users, total_score = extract_all_users(users)
    combined_html = generate_html_report(merged_report["start_date"],
                                         merged_report["end_date"],
                                         users, all_users, total_score)
    return combined_html

if __name__ == "__main__":
    # 네 개의 결과물 파일 (파일명은 실제 파일명으로 변경)
    files = [
        "dd.txt",       # 집계 기간: 2025/02/01 00:00:00 ~ 2025/02/08 00:00:00
        "dddddd.txt",   # 집계 기간: 2025/02/08 00:00:00 ~ 2025/02/14 00:00:00
        "",             # 빈 파일 경로 예시
        "ddd.txt"     # 예시 파일 4
    ]
    combined_report_html = merge_reports_from_multiple_files(files)
    print(combined_report_html)
