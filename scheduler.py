# random_scheduler.py

import schedule
import time
import random
from main import process_clients_daily

def random_time():
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    return f"{hour:02d}:{minute:02d}"

def schedule_daily_random():
    time_str = random_time()
    print(f"Scheduling task at {time_str}")
    schedule.every().day.at(time_str).do(process_clients_daily)

schedule_daily_random()

while True:
    schedule.run_pending()
    time.sleep(1)
    if schedule.idle_seconds() <= 60:  # 하루가 지나가기 전에 새로운 랜덤 시간을 예약합니다.
        schedule.clear()
        schedule_daily_random()
