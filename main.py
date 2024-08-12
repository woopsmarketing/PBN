# content_uploader.py
import requests
from wordpress_xmlrpc import Client, WordPressPost
from wordpress_xmlrpc.methods import media, posts
from wordpress_xmlrpc.compat import xmlrpc_client
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import random
from selenium import webdriver
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor
from DBmanager import get_all_pbn_sites, get_client_keywords, save_post_url, get_random_keyword, get_client_site, get_daily_requests, get_remaining_extra_backlinks
import re
import urllib.request
import ssl
import time
from concurrent.futures import ThreadPoolExecutor
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv('OPENAI_API_KEY')


# OpenAI API 설정
client = OpenAI(api_key=api_key)


def get_image_url_from_unsplash(keyword):
    access_key = 'Vu12NfV6QG0P1FT-sR1HERwZK7kVyHY75K-Pe4KfJXI'
    base_url = "https://api.unsplash.com/photos/random"

    def fetch_image(query):
        url = f"{base_url}?query={query}&client_id={access_key}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data['urls']['regular']
        else:
            print(
                f"Failed to fetch image from Unsplash: Status {response.status_code} for keyword: {query}")
            return None

    # 먼저 주어진 키워드로 이미지를 검색
    image_url = fetch_image(keyword)

    # 주어진 키워드로 이미지를 찾지 못한 경우 대체 키워드로 검색
    if not image_url:
        alternate_keywords = ["스포츠", "sports", "배경"]
        for alt_keyword in alternate_keywords:
            image_url = fetch_image(alt_keyword)
            if image_url:
                break

    return image_url


def generate_blog_title(keyword):
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "you are a very creative blogger"},
            {"role": "user", "content": f"Create a creative blog title for your keyword in Korean: {keyword}"}
        ]
    )
    title_message = response.choices[0].message
    title = title_message.content.strip()
    title = title.replace('"', '').replace("'", "")  # 따옴표 제거
    print(title)
    return title


def upload_image_to_wordpress(image_url, wp_client, keyword):
    if not image_url:
        return None, None

    try:
        # SSL 인증서 검증을 비활성화하는 SSL 컨텍스트 생성
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(image_url, context=context) as response:
            image_data = response.read()

        data = {
            'name': f"{keyword}.jpg",
            'type': 'image/jpeg',
            'bits': xmlrpc_client.Binary(image_data),
        }
        response = wp_client.call(media.UploadFile(data))
        return response['id'], response['link']
    except Exception as e:
        print(f"Failed to upload image to WordPress: {e}")
        return None, None


def generate_long_blog_content(title, keyword, wp_client, client_id, desired_length=400):
    content = ""
    prompt = f"'{title}' 이 제목을 포함하여 SEO에 유용한 블로그 콘텐츠를 한글로 작성해주세요. 결론, 마무리, 끝으로, 마치며 등과 같은 마지막 부제목은 사용하지 말아주세요. 생성된 콘텐츠를 이어서 추가로 콘텐츠를 추가할 것입니다.\n\n"

    while len(content.split()) < desired_length:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a great SEO expert."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=4096
        )
        new_content = response.choices[0].message
        new_content = new_content.content.strip()

        # H 태그와 굵은 글씨 변환
        new_content = re.sub(r'###### (.*)', r'<h6>\1</h6>', new_content)
        new_content = re.sub(r'##### (.*)', r'<h5>\1</h5>', new_content)
        new_content = re.sub(r'#### (.*)', r'<h4>\1</h4>', new_content)
        new_content = re.sub(r'### (.*)', r'<h3>\1</h3>', new_content)
        new_content = re.sub(r'## (.*)', r'<h2>\1</h2>', new_content)
        new_content = re.sub(r'# (.*)', r'<h1>\1</h1>', new_content)
        new_content = re.sub(
            r'\*\*(.*?)\*\*', r'<strong>\1</strong>', new_content)

        # 문단을 <p> 태그로 감싸기
        new_content = re.sub(r'(?<!</p>)\n(?!<p>)',
                             r'<br>', new_content)  # 줄바꿈 처리
        new_content = '<p>' + \
            new_content.replace('\n', '</p><p>') + '</p>'  # 문단 처리

        print(new_content)
        content += " " + str(new_content)
        print(str(len(content.split())) + " -------- " + str(desired_length))

        if len(content.split()) >= desired_length:
            break
        prompt = new_content + \
            "\n\n블로그 게시글을 이어서 작성해주세요. 결론, 마무리, 끝으로, 마치며 등과 같은 마지막 부제목은 사용하지 말아주세요.\n\n"

    # 마지막에 결론 요청
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a great SEO expert."},
            {"role": "user", "content": content + "\n\n결론 부분을 작성해주세요."}
        ],
        max_tokens=4096
    )
    conclusion_content = response.choices[0].message
    conclusion_content = conclusion_content.content.strip()

    # H 태그와 굵은 글씨 변환
    conclusion_content = re.sub(
        r'###### (.*)', r'<h6>\1</h6>', conclusion_content)
    conclusion_content = re.sub(
        r'##### (.*)', r'<h5>\1</h5>', conclusion_content)
    conclusion_content = re.sub(
        r'#### (.*)', r'<h4>\1</h4>', conclusion_content)
    conclusion_content = re.sub(
        r'### (.*)', r'<h3>\1</h3>', conclusion_content)
    conclusion_content = re.sub(r'## (.*)', r'<h2>\1</h2>', conclusion_content)
    conclusion_content = re.sub(r'# (.*)', r'<h1>\1</h1>', conclusion_content)
    conclusion_content = re.sub(
        r'\*\*(.*?)\*\*', r'<strong>\1</strong>', conclusion_content)

    # 문단을 <p> 태그로 감싸기
    conclusion_content = re.sub(
        r'(?<!</p>)\n(?!<p>)', r'<br>', conclusion_content)  # 줄바꿈 처리
    conclusion_content = '<p>' + \
        conclusion_content.replace('\n', '</p><p>') + '</p>'  # 문단 처리

    content += conclusion_content
    print(content)
    keyword_anchor = f'<a href="{get_client_site(client_id)}" target="_blank" rel="noopener" title="{keyword}" aria-label="{keyword}">{keyword}</a>'
    split_content = content.split()
    insert_position = random.randint(0, len(split_content))
    split_content.insert(insert_position, keyword_anchor)
    content_with_keyword = " ".join(split_content)

    image_url = get_image_url_from_unsplash(keyword)
    # image_id, image_src = upload_image_to_wordpress(
    #     image_url, wp_client, keyword)

    if image_url:
        image_tag = f'<img src="{image_url}" alt="{keyword}" title="{keyword}" loading="lazy">'
        content_with_image = f"{image_tag}\n\n{content_with_keyword}"
        return content_with_image
    else:
        return content_with_keyword


def post_to_wordpress(wp_client, title, content):
    try:
        post = WordPressPost()
        post.title = title
        post.content = content
        post.post_status = 'publish'
        post.terms_names = {
            'post_tag': ['example', 'post'],
            'category': ['Uncategorized']
        }
        post_id = wp_client.call(posts.NewPost(post))
        return post_id
    except Exception as e:
        print(f"Failed to post to WordPress: {e}")
        return None


def create_backlink(client_id):
    pbn_sites = get_all_pbn_sites()
    pbn_site = random.choice(pbn_sites)

    keyword = get_random_keyword(client_id)

    if not keyword:
        print(f"Client {client_id} has no keywords.")
        return

    title = generate_blog_title(keyword)

    # Ensure the URL has a protocol
    if not pbn_site[1].startswith("http://") and not pbn_site[1].startswith("https://"):
        pbn_site_url = "https://" + pbn_site[1]  # Default to https://
    else:
        pbn_site_url = pbn_site[1]
    print(pbn_site_url)
    wp_client = Client(f"{pbn_site_url}/xmlrpc.php", pbn_site[2], pbn_site[3])
    print(wp_client)
    content = generate_long_blog_content(
        title, keyword, wp_client, client_id, desired_length=400)

    post_id = post_to_wordpress(wp_client, title, content)

    if post_id:
        post_url = f"{pbn_site_url}?p={post_id}"
        save_post_url(client_id, get_client_site(client_id), keyword, post_url)
        print(
            f"Backlink created for client {client_id} on {pbn_site[1]} with keyword '{keyword}'")
    else:
        print(
            f"Failed to create backlink for client {client_id} on {pbn_site[1]}")


# def process_client_backlinks(client_id, daily_backlinks):
#     for _ in range(daily_backlinks):
#         create_backlink(client_id)
#         time.sleep(random.randint(60, 120))
#     # Handle remaining extra backlinks
#     if get_remaining_extra_backlinks(client_id) > 0:
#         create_backlink(client_id)
#         time.sleep(random.randint(60, 120))

# # 함수 병렬처리( 동시에 여러 클라이언트를 마구잡이로 실행하기위해.)


# def process_clients_daily():
#     daily_requests = get_daily_requests()

#     with ThreadPoolExecutor(max_workers=len(daily_requests)) as executor:
#         futures = []
#         for client_id, daily_backlinks in daily_requests:
#             futures.append(executor.submit(
#                 process_client_backlinks, client_id, daily_backlinks))
#         for future in futures:
#             future.result()  # Ensure all threads have completed

def calculate_daily_backlinks(total_backlinks, remaining_days):
    # 남은 백링크와 남은 날을 기반으로 하루 할당량을 계산합니다.
    if remaining_days > 0:
        return total_backlinks // remaining_days
    return total_backlinks


def login_to_wordpress(driver, site_url, username, password):
    # WordPress 로그인 페이지 열기
    driver.get(f"{site_url}/wp-login.php")

    # 사용자 이름 입력
    WebDriverWait(driver, 10).until(EC.presence_of_element_located(
        (By.ID, "user_login"))).send_keys(username)

    # 비밀번호 입력
    driver.find_element(By.ID, "user_pass").send_keys(password)

    # 로그인 버튼 클릭
    driver.find_element(By.ID, "wp-submit").click()

    # 로그인 성공 여부 확인
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "wpadminbar")))


def create_backlink_selenium(driver, client_id):
    pbn_sites = get_all_pbn_sites()
    pbn_site = random.choice(pbn_sites)
    pbn_site_url, username, password = pbn_site[1], pbn_site[2], pbn_site[3]

    keyword = get_random_keyword(client_id)
    if not keyword:
        print(f"Client {client_id} has no keywords.")
        return

    title = f"Generated Blog Title for {keyword}"  # 여기에 타이틀 생성 로직을 추가하세요.
    # 여기에 콘텐츠 생성 로직을 추가하세요.
    content = f"<p>Content generated for keyword: {keyword}</p>"

    # WordPress 로그인
    login_to_wordpress(driver, pbn_site_url, username, password)

    # 글 작성 페이지로 이동
    driver.get(f"{pbn_site_url}/wp-admin/post-new.php")

    # 제목 입력
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "title"))).send_keys(title)

    # 내용 입력
    driver.find_element(By.ID, "content").send_keys(content)

    # 게시 버튼 클릭
    driver.find_element(By.ID, "publish").click()

    # 게시물 URL 확인
    WebDriverWait(driver, 10).until(EC.presence_of_element_located(
        (By.CLASS_NAME, "components-notice__content")))
    post_url = driver.find_element(
        By.CLASS_NAME, "components-notice__content a").get_attribute('href')

    save_post_url(client_id, get_client_site(client_id), keyword, post_url)
    print(
        f"Backlink created for client {client_id} on {pbn_site_url} with keyword '{keyword}'")


def process_clients_daily():
    # 매일 처리해야 하는 요청을 가져옵니다.
    daily_requests = get_daily_requests()

    # 오늘 처리해야 할 모든 백링크 작업을 무작위로 섞기 위한 리스트
    task_list = []

    # 클라이언트별 할당량을 기반으로 작업 리스트 생성
    for client_id, daily_backlinks in daily_requests:
        remaining_extra_backlinks = get_remaining_extra_backlinks(client_id)
        total_backlinks = daily_backlinks + remaining_extra_backlinks

        # 남은 일수를 고려하여 새로운 하루 할당량 계산
        remaining_days = len(daily_requests)  # 남은 날 계산 (이 로직은 필요에 따라 조정 가능)
        actual_daily_backlinks = calculate_daily_backlinks(
            total_backlinks, remaining_days)

        task_list.extend([client_id] * actual_daily_backlinks)

    # 작업 리스트를 무작위로 섞습니다.
    random.shuffle(task_list)

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Headless 모드 사용
    driver = webdriver.Chrome(options=options)

    # 섞인 작업 리스트를 순차적으로 처리합니다.
    for client_id in task_list:
        try:
            create_backlink(driver, client_id)
        except Exception as e:
            print(f"Error creating backlink for client {client_id}: {e}")
            continue
        time.sleep(random.randint(60, 120))  # 작업 간의 대기 시간

    # 남은 추가 백링크 처리
    for client_id, _ in daily_requests:
        remaining_backlinks = get_remaining_extra_backlinks(client_id)
        while remaining_backlinks > 0:
            try:
                create_backlink(driver, client_id)
                remaining_backlinks -= 1
            except Exception as e:
                print(
                    f"Error creating extra backlink for client {client_id}: {e}")
                break  # 추가 작업 중 오류가 발생하면 해당 클라이언트의 작업을 중단
            time.sleep(random.randint(60, 120))  # 작업 간의 대기 시간

    driver.quit()


if __name__ == "__main__":
    process_clients_daily()
