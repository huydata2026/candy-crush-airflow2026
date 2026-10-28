import json
import urllib.request
from datetime import datetime

from airflow.sdk import DAG, Param, task
from google.cloud import bigquery

from candycrush import dates
from candycrush.config import DEFAULT_ARGS
from candycrush.sql_builder import build_split_sql, flat_table, load_events, render_flat_sql

DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1563375727221416018/6RI6EPJMuBeAIGrdNjscdW2fyaPg7-DAezQzLTlngObPzUrghHB7gGOcXzNWUlsg0i0_"


def send_discord(message: str):
    req = urllib.request.Request(
        DISCORD_WEBHOOK_URL,
        data=json.dumps({"content": message}).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "airflow"},
    )
    urllib.request.urlopen(req, timeout=10)


def notify_success(context):
    send_discord(f"DAG {context['dag'].dag_id} chạy thành công")


def notify_failure(context):
    send_discord(f"DAG {context['dag'].dag_id} thất bại")


with DAG(
    dag_id="11_etl_flat_event_candycrushsaga_discord",
    start_date=datetime(2026, 10, 1),
    schedule="15 9 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
    tags=["candy_crush", "flat_event", "discord"],
    params={"run_date": Param("", type="string", description="YYYYMMDD, để trống = hôm qua")},
) as dag:

    @task
    def resolve_run_date(params) -> str:
        return dates.resolve_run_date(params["run_date"])

    @task
    def create_flat_table(run_date: str):
        bigquery.Client().query(render_flat_sql(run_date)).result()

    @task
    def split_table_by_event(run_date: str):
        client = bigquery.Client()
        for event in load_events():
            client.query(build_split_sql(event, run_date)).result()

    @task
    def check_data(run_date: str):
        sql = f"SELECT COUNT(*) AS row_count FROM `{flat_table(run_date)}`"
        row_count = next(iter(bigquery.Client().query(sql).result())).row_count
        if row_count == 0:
            raise ValueError(f"Bảng flat_event_{run_date} không có dữ liệu!")

    run_date = resolve_run_date()
    create_flat_table(run_date) >> split_table_by_event(run_date) >> check_data(run_date)
