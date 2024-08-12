import sqlite3
import pandas as pd
from xlsxwriter import Workbook


def fetch_posts_from_db():
    conn = sqlite3.connect('backlink_manager.db')
    query = """
    SELECT p.post_id, p.client_id, c.client_name, p.client_site, p.keyword, p.post_url
    FROM posts p
    JOIN clients c ON p.client_id = c.client_id
    """
    posts_df = pd.read_sql_query(query, conn)
    conn.close()
    return posts_df


def save_to_excel(posts_df):
    writer = pd.ExcelWriter('backlink_report.xlsx', engine='xlsxwriter')
    posts_df.to_excel(writer, sheet_name='Posts', index=False)

    # 클라이언트 별로 분류하기
    clients = posts_df['client_id'].unique()
    for client_id in clients:
        client_posts = posts_df[posts_df['client_id'] == client_id]
        # Assuming client_name is in the posts table
        client_name = client_posts['client_name'].iloc[0]
        client_posts.to_excel(
            writer, sheet_name=f'Client_{client_id}_{client_name}', index=False)

    writer.close()


if __name__ == "__main__":
    posts_df = fetch_posts_from_db()
    save_to_excel(posts_df)
    print("Excel report generated successfully.")
