# db_manager.py
import sqlite3
import random
import pandas as pd

def init_db():
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()
    
    # 고객 정보 테이블 생성
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS clients (
        client_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        pbn_site TEXT NOT NULL,
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

def add_client(site_url, total_backlinks):
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO clients (site_url, total_backlinks)
    VALUES (?, ?)
    ''', (site_url, total_backlinks))
    
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
    
    cursor.execute('SELECT keyword FROM client_keywords WHERE client_id = ?', (client_id,))
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
    
    cursor.execute('SELECT site_url FROM clients WHERE client_id = ?', (client_id,))
    site_url = cursor.fetchone()
    conn.close()
    return site_url[0] if site_url else None

def save_post_url(client_id, pbn_site, post_url):
    conn = sqlite3.connect('backlink_manager.db')
    cursor = conn.cursor()
    
    cursor.execute('''
    INSERT INTO posts (client_id, pbn_site, post_url)
    VALUES (?, ?, ?)
    ''', (client_id, pbn_site, post_url))
    
    cursor.execute('''
    UPDATE clients
    SET completed_backlinks = completed_backlinks + 1
    WHERE client_id = ?
    ''', (client_id,))
    
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
    
    cursor.execute('''
    SELECT client_id, daily_backlinks FROM client_requests WHERE remaining_days > 0
    ''')
    daily_requests = cursor.fetchall()
    
    cursor.execute('''
    UPDATE client_requests SET remaining_days = remaining_days - 1
    WHERE remaining_days > 0
    ''')
    
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

if __name__ == "__main__":
    init_db()
    
    # client_id = 1  # 현재 테이블에 있는 클라이언트 ID를 사용합니다.
    # total_backlinks = 100
    # days = 10

    # add_client_request(client_id, total_backlinks, days)

    # 테이블 상태 확인
    print("\nClient Requests Table")
    print(view_table("client_requests"))

    print("--------------------------------------------")
    # 데이터베이스 초기화 후 예시 데이터 추가
    # client_id = add_client("https://example.com", 100)
    # keywords = ["keyword1", "keyword2", "keyword3"]
    # for keyword in keywords:
    #     add_client_keyword(client_id, keyword)
    # add_client_request(client_id, 100, 10)
    
    # add_pbn_site("example-pbn.com", "admin", "password")

    # 테이블 상태 확인
    print("Clients Table")
    print(view_table("clients"))
    
    print("\nClient Keywords Table")
    print(view_table("client_keywords"))
    
    print("\nClient Requests Table")
    print(view_table("client_requests"))
    
    print("\nPBN Sites Table")
    print(view_table("pbn_sites"))
    
    print("\nPosts Table")
    print(view_table("posts"))
