import logging

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def _say_hello():
    logging.info("Hello SAKSIAM!")


def _log_message():
    logging.info('This is log message')


default_args = {
    "owner": "Maxine",
    "start_date": timezone.datetime(2021, 9, 27),
}
with DAG(
    "my_dag_hw1_2",
    schedule_interval="*/30 * * * *",
    default_args=default_args,
    catchup=False,
    tags=["saksiam"],
) as dag:

    start = DummyOperator(task_id="start")

    echo_hello = BashOperator(
        task_id="echo_hello",
        bash_command="echo hello",
    )

    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=_say_hello,
    )

    print_log_message = PythonOperator(
        task_id="print_log_message",
        python_callable=_log_message,
    )

    end = DummyOperator(task_id="end")

    start >> echo_hello >> say_hello >> print_log_message >> end
