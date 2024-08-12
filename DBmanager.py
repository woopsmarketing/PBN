# db_manager.py
import sqlite3
import random
import pandas as pd
from datetime import datetime


def init_db():
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()

    # 고객 정보 테이블 생성
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS clients (
        client_id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_name TEXT NOT NULL,
        site_url TEXT NOT NULL,
        total_backlinks INTEGER NOT NULL,
        completed_backlinks INTEGER DEFAULT 0
    )
    ''')

    # 고객 키워드 테이블 생성
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS client_keywords (
        keyword_id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER NOT NULL,
        keyword TEXT NOT NULL,
        FOREIGN KEY (client_id) REFERENCES clients(client_id)
    )
    ''')

    # PBN 정보 테이블 생성
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS pbn_sites (
        site_id INTEGER PRIMARY KEY AUTOINCREMENT,
        domain TEXT NOT NULL,
        username TEXT NOT NULL,
        password TEXT NOT NULL
    )
    ''')

    # 백링크 정보 테이블 생성
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS posts (
        post_id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER NOT NULL,
        client_site TEXT NOT NULL,
        keyword TEXT NOT NULL,
        post_url TEXT NOT NULL,
        FOREIGN KEY (client_id) REFERENCES clients(client_id)
    )
    ''')

    # 고객 요청 테이블 생성
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS client_requests (
        request_id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER NOT NULL,
        total_backlinks INTEGER NOT NULL,
        daily_backlinks INTEGER NOT NULL,
        extra_backlinks INTEGER NOT NULL,
        remaining_days INTEGER NOT NULL,
        FOREIGN KEY (client_id) REFERENCES clients(client_id)
    )
    ''')

    conn.commit()
    conn.close()


def view_table(table_name):
    conn = sqlite3.connect('backlink_manager.db')
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df


def add_pbn_site(domain, username, password):
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()

    cursor.execute('''
    INSERT INTO pbn_sites (domain, username, password)
    VALUES (?, ?, ?)
    ''', (domain, username, password))

    conn.commit()
    conn.close()


def get_all_pbn_sites():
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM pbn_sites')
    pbn_sites = cursor.fetchall()
    conn.close()
    return pbn_sites


def add_client(client_name, site_url, total_backlinks):
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()

    cursor.execute('''
    INSERT INTO clients (client_name, site_url, total_backlinks)
    VALUES (?, ?, ?)
    ''', (client_name, site_url, total_backlinks))

    client_id = cursor.lastrowid  # 마지막으로 삽입된 행의 ID를 반환합니다.

    conn.commit()
    conn.close()

    return client_id


def add_client_keyword(client_id, keyword):
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()

    cursor.execute('''
    INSERT INTO client_keywords (client_id, keyword)
    VALUES (?, ?)
    ''', (client_id, keyword))

    conn.commit()
    conn.close()


def get_all_clients():
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM clients')
    clients = cursor.fetchall()
    conn.close()
    return clients


def get_client_keywords(client_id):
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()

    cursor.execute(
        'SELECT keyword FROM client_keywords WHERE client_id = ?', (client_id,))
    keywords = cursor.fetchall()
    conn.close()
    return keywords


def get_random_keyword(client_id):
    keywords = get_client_keywords(client_id)
    if keywords:
        return random.choice(keywords)[0]
    else:
        return None


def get_client_site(client_id):
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()

    cursor.execute(
        'SELECT site_url FROM clients WHERE client_id = ?', (client_id,))
    site_url = cursor.fetchone()
    conn.close()
    return site_url[0] if site_url else None


def save_post_url(client_id, client_site, keyword, post_url):
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()

    cursor.execute('''
    INSERT INTO posts (client_id, client_site, keyword, post_url)
    VALUES (?, ?, ?, ?)
    ''', (client_id, client_site, keyword, post_url))

    cursor.execute('''
    UPDATE clients
    SET completed_backlinks = completed_backlinks + 1
    WHERE client_id = ?
    ''', (client_id,))

    conn.commit()
    conn.close()


def reset_remaining_days(client_id, days):
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()

    cursor.execute('''
    UPDATE client_requests SET remaining_days = ? WHERE client_id = ?
    ''', (days, client_id))

    conn.commit()
    conn.close()


def add_client_request(client_id, total_backlinks, days):
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()

    daily_backlinks = total_backlinks // days
    extra_backlinks = total_backlinks % days

    cursor.execute('''
    INSERT INTO client_requests (client_id, total_backlinks, daily_backlinks, extra_backlinks, remaining_days)
    VALUES (?, ?, ?, ?, ?)
    ''', (client_id, total_backlinks, daily_backlinks, extra_backlinks, days))

    conn.commit()
    conn.close()


def get_daily_requests():
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()

    # 오늘 날짜 가져오기
    today = datetime.now().date()

    # 클라이언트별로 remaining_days가 오늘 업데이트된 적이 있는지 확인
    cursor.execute('''
    SELECT client_id, last_update FROM client_requests
    ''')
    client_updates = cursor.fetchall()

    # 남은 일수를 업데이트해야 하는 클라이언트 식별
    clients_to_update = [
        client_id for client_id, last_update in client_updates
        if not last_update or datetime.strptime(last_update, "%Y-%m-%d").date() < today
    ]

    # 남은 일수를 업데이트해야 하는 경우만 업데이트
    if clients_to_update:
        cursor.execute('''
        UPDATE client_requests
        SET remaining_days = remaining_days - 1, last_update = ?
        WHERE client_id IN ({seq})
        '''.format(seq=','.join(['?'] * len(clients_to_update))),
            [today] + clients_to_update)

    # remaining_days가 0보다 큰 요청을 가져옴
    cursor.execute('''
    SELECT client_id, daily_backlinks FROM client_requests WHERE remaining_days > 0
    ''')
    daily_requests = cursor.fetchall()

    conn.commit()
    conn.close()

    return daily_requests


def get_remaining_extra_backlinks(client_id):
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()

    cursor.execute('''
    SELECT extra_backlinks FROM client_requests WHERE client_id = ?
    ''', (client_id,))
    result = cursor.fetchone()

    if result:
        extra_backlinks = result[0]
        if extra_backlinks > 0:
            cursor.execute('''
            UPDATE client_requests SET extra_backlinks = extra_backlinks - 1 WHERE client_id = ?
            ''', (client_id,))
            conn.commit()
        conn.close()
        return extra_backlinks
    else:
        conn.close()
        return 0


def add_last_update_column():
    # SQLite 데이터베이스에 연결
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()

    # 테이블에 last_update 열 추가
    cursor.execute('''
    ALTER TABLE client_requests ADD COLUMN last_update TEXT;
    ''')

    # 변경 사항 커밋하고 연결 종료
    conn.commit()
    conn.close()

    print("Column 'last_update' added successfully.")


if __name__ == "__main__":
    init_db()

    # 데이터베이스 초기화 후 예시 데이터 추가
    # client_id = add_client("토토오톡", "https://ttot1004.com", 120)
    # keywords = ["스포츠무료중계", "스포츠무료티비", "무료스포츠중계", "무료스포츠티비", "토토정보방", "토토정보방 토토오톡", "무료중계사이트", "스포츠토토커뮤니티", "한국축구무료티비", "토톡", "토토오", "토토오톡", "토톡",
    #             "무료중계 토토오톡티비", "토토오톡티비", "스포츠무료티비 토토오톡티비", "스포츠무료중계 토토오톡티비", "무료실시간토토오톡티비", "토토사이트", "실시간무료티비 토토오톡", "토토커뮤니티", "토토정보", "메이저사이트 토토오톡", "토지노 토토오톡", "꽁머니"]
    # for keyword in keywords:
    #     add_client_keyword(client_id, keyword)
    # add_client_request(client_id, 120, 3)
    # client_id_2 = add_client(
    #     "청년건물주", "https://criminal-lawfirm-dongju.com", 60)
    # keywords_2 = ["강제추행변호사", "성추행처벌", "지하철성추행", "화장실몰카", "몰카범처벌", "불법촬영처벌"]
    # for keyword in keywords_2:
    #     add_client_keyword(client_id_2, keyword)
    # add_client_request(client_id_2, 60, 5)
    # add_pbn_site("yururira-blog.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("gamecubedlx.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("realfooddiets.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("volantsports.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("secondmassage.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("croadriainvest.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("maihiendidongnghean.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("margiesmassage.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("donofan.org", "admin", "Lqp1o2k3!")
    # add_pbn_site("cheryhardcore.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("spam-news.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("easyridersdanang.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("magicshop22.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("dailydoseofsales.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("uniqecasino.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("totoagc.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("andybakerlive.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("hvslive.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("justlygamble.com", "admin", "Lqp1o2k3!")
    # add_pbn_site("futuresportsedition.com", "admin", "Lqp1o2k3!")
    # reset_remaining_days(1, 3)
    # 테이블 상태 확인
    print("\nClient Requests Table")
    print(view_table("client_requests"))

    print("--------------------------------------------")
    print("Clients Table")
    print(view_table("clients"))

    print("\nClient Keywords Table")
    print(view_table("client_keywords"))

    print("\nPBN Sites Table")
    print(view_table("pbn_sites"))

    print("\nPosts Table")
    print(view_table("posts"))
