import pickle
from datetime import timedelta, datetime

def load_pickle_data(file_path: str) -> dict:
    """
    지정한 파일에서 pickle 데이터를 로드합니다.
    """
    with open(file_path, "rb") as fr:
        data = pickle.load(fr)
    return data

def aggregate_user_data(rank: dict) -> dict:
    """
    rank 딕셔너리(키: "닉네임(user_id)" 형태)를 기반으로
    사용자 ID별로 글, 댓글 수를 합산하고 사용자의 닉네임 집합을 구성합니다.
    """
    idrank = {}
    for key in rank:
        # key의 형식이 "닉네임(user_id)"라고 가정하여 '('를 기준으로 분리합니다.
        parts = key.split('(')
        if len(parts) <= 1:
            continue  # 형식이 맞지 않으면 건너뜁니다.
        nick = parts[0]  # 닉네임
        user_id = parts[1][:-1]  # 마지막 ')' 제거하여 user_id 추출

        # 사용자 데이터 초기화
        if user_id not in idrank:
            idrank[user_id] = {"nicks": set(), "article": 0, "reply": 0}

        # 글과 댓글 수 누적
        idrank[user_id]["article"] += int(rank[key]["article"])
        idrank[user_id]["reply"] += int(rank[key]["reply"])

        # 닉네임이 "ㅤ" 또는 끝에 '..'가 붙은 경우 제외하고 추가
        if nick != "ㅤ" and nick[-2:] != '..':
            idrank[user_id]["nicks"].add(nick)

    return idrank

def determine_date_range(data: dict, rank: dict) -> (datetime, datetime):
    """
    집계 기간을 결정합니다.
    
    1. data 딕셔너리에 'start_date'와 'end_date'가 있으면 그 값을 사용합니다.
    2. 없으면 rank 내의 각 항목에 "date" 키가 있으면 그 중 최소/최대 값을 사용합니다.
    3. 둘 다 없으면 fallback으로 data["date"] 또는 현재 시간을 기준으로 사용합니다.
    """
    if "start_date" in data and "end_date" in data:
        start_dt = data["start_date"]
        end_dt = data["end_date"]
    else:
        dates = []
        for entry in rank.values():
            if "date" in entry:
                dates.append(entry["date"])
        if dates:
            start_dt = min(dates)
            end_dt = max(dates)
        else:
            fallback = data.get("date", datetime.now())
            start_dt = fallback - timedelta(days=1)
            end_dt = fallback
    return start_dt, end_dt

def calculate_scores_and_ranks(idrank: dict) -> None:
    """
    각 사용자에 대해 총 점수를 계산(글 3점, 댓글 1점)하고,
    글 수와 댓글 수에 따른 순위를 idrank 딕셔너리에 추가합니다.
    """
    # 총 점수 계산
    for user_id in idrank:
        idrank[user_id]["score"] = idrank[user_id]["article"] * 3 + idrank[user_id]["reply"]

    # 글 수 기준 순위 계산
    sorted_by_article = sorted(idrank.items(), key=lambda x: x[1]["article"], reverse=True)
    rank_num = 1
    for i, (user_id, data) in enumerate(sorted_by_article):
        if i != 0 and sorted_by_article[i - 1][1]["article"] != data["article"]:
            rank_num += 1
        idrank[user_id]["articleRank"] = rank_num

    # 댓글 수 기준 순위 계산
    sorted_by_reply = sorted(idrank.items(), key=lambda x: x[1]["reply"], reverse=True)
    rank_num = 1
    for i, (user_id, data) in enumerate(sorted_by_reply):
        if i != 0 and sorted_by_reply[i - 1][1]["reply"] != data["reply"]:
            rank_num += 1
        idrank[user_id]["replyRank"] = rank_num

def extract_all_users(idrank: dict) -> (list, float):
    """
    총 점수를 기준으로 내림차순 정렬하여 모든 사용자를 리스트로 반환합니다.
    또한 전체 사용자들의 총 점수를 함께 반환합니다.
    """
    sorted_by_score = sorted(idrank.items(), key=lambda x: x[1]["score"], reverse=True)
    all_users = []
    rank_num_final = 1
    for i, (user_id, user_data) in enumerate(sorted_by_score):
        if i != 0 and sorted_by_score[i - 1][1]["score"] != user_data["score"]:
            rank_num_final += 1
        all_users.append((rank_num_final, user_id, user_data))
    total_score = sum(user_data["score"] for (_, _, user_data) in all_users)
    return all_users, total_score

def generate_html_report(startdate: str, enddate: str, idrank: dict, all_users: list, total_score: float) -> str:
    """
    HTML 형식의 보고서를 생성합니다.
    """
    # 전체 글/댓글 수 계산
    total_articles = sum(user_data["article"] for user_data in idrank.values())
    total_replies = sum(user_data["reply"] for user_data in idrank.values())
    total_users = len(idrank)  # 전체 사용자 수 계산

    html_lines = [
        "<html>",
        "<head>",
        '    <meta charset="UTF-8">',
        "    <title>집계 결과</title>",
        "</head>",
        '<body style="font-size:12px; font-family: sans-serif;">',
        f"    <p>집계 기간 : {startdate} ~ {enddate}</p>",
        "    <p>(글+3점, 댓글+1점)</p>",
        f"    <p>총 글 수: {total_articles}개, 총 댓글 수: {total_replies}개, 총 갤러 수: {total_users}명</p>"
    ]

    # 사용자 목록 출력
    for rank_val, user_id, user_data in all_users:
        # 출력할 닉네임 필터링 (특정 값 제외)
        nicks_to_print = [nick for nick in user_data["nicks"] if nick != "ㅤ"]
        share = (user_data["score"] / total_score * 100) if total_score else 0
        html_lines.append(
            f"    <p>{rank_val}등 : {','.join(nicks_to_print)}({user_id}) | 총 점: {user_data['score']}점 | "
            f"갤 지분(총점): {share:.2f}% | 글 {user_data['article']}개({user_data['articleRank']}위), "
            f"댓글 {user_data['reply']}개({user_data['replyRank']}위)</p>"
        )
        html_lines.append("    <hr>")

    html_lines.extend([
        "</body>",
        "</html>"
    ])

    return "\n".join(html_lines)

def main():
    # 1. Pickle 파일에서 데이터 로드
    data = load_pickle_data("data.pickle")

    # 2. 사용자별 집계: 닉네임과 글/댓글 수 누적
    rank_data = data["rank"]
    idrank = aggregate_user_data(rank_data)

    # 3. 집계 기간 결정
    start_dt, end_dt = determine_date_range(data, rank_data)
    startdate_str = start_dt.strftime('%Y/%m/%d %H:%M:%S')
    enddate_str = end_dt.strftime('%Y/%m/%d %H:%M:%S')

    # 4. 총 점수 계산 및 글/댓글 순위 부여 (글: 3점, 댓글: 1점)
    calculate_scores_and_ranks(idrank)

    # 5. 총 점수를 기준으로 모든 사용자 추출
    all_users, total_score = extract_all_users(idrank)

    # 6. HTML 보고서 생성
    html_report = generate_html_report(startdate_str, enddate_str, idrank, all_users, total_score)

    # 7. 결과 출력 (또는 파일로 저장 가능)
    print(html_report)

if __name__ == "__main__":
    main()
