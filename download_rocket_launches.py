import json
import pathlib
import airflow
import requests 
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id='download_rocket_launches',  # DAG 이름
    start_date=airflow.utils.dates.days_ago(14),  # 시작 날짜
    schedule_interval= "@daily",  # 실행 간격
)

# BashOperator를 이용해 URL로부터 데이터를 다운로드
download_launches = BashOperator(
    task_id='download_launches',  # 테스크 이름
    bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag=dag
)

# 파이썬 함수를 사용하여 이미지 다운로드
def _get_pictures():
    # 경로가 존재하는지 확인
    images_path = pathlib.Path("/tmp/images")
    if not images_path.exists():
        images_path.mkdir(parents=True)

    # launches.json 파일을 열고 이미지 다운로드
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, 'wb') as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f'{image_url} appears to be an invalid URL.')
            except requests_exceptions.ConnectionError:
                print(f'Could not connect to {image_url}.')

# PythonOperator를 사용하여 파이썬 함수 실행
get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable=_get_pictures,
    dag=dag
)

# 이미지 개수를 알리는 BashOperator
notify = BashOperator(
    task_id='notify',
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag
)

# DAG의 실행 순서 정의
download_launches >> get_pictures >> notify
