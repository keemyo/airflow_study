# (예약 안된) 이벤트 DAG의 버전 초기화(dag/01_unscheduled.py)
import datetime as dt from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operator.bash import BashOperator 
from airflow.operator.python import PythonOperator

dag = DAG(
    dag_id = "01_unscheduled",    
    start_date = dt.datetime(year=2019, month=1, day=1),
    end_date=dt.datetime(year=2019, month=1, day=5)    
)

fetch_events = BashOperator(
    task_id = "fetch_events",
    bash_command = (
        "mkdir -p /data &&"
        "curl -o /data/events.json"
        "https://localhost:5000/events" # API에서 이벤트를 가져온 후 저장
    ),
    dag=dag,
)

def _calculate_stats(input_path, output_path):
    """이벤트 통계 계산기"""
    events = pd.read_json(input_path)
    stats = events.groupby(['date', 'user']).size().reset_index() # 이벤트 데이터를 로드하고 필요한 통계를 계산 
    path(output_path).parent.mkdir(exist_ok=True) # 추ㄹ력 디렉토리가 있는지 확인하고 결과를 csv로 저장
    stats.to_csv(output_path, index=False) 

calculate_stats = PythonOperator(
    task_id = "calculate_stats",
    python_callable = _calculate_stats,
    op_kwargs = {
        "input_path": "/data/events.json",
        "output_path": "/data_stats.csv"
    },
    dag = dag
)

fetch_events >> calculate_stats