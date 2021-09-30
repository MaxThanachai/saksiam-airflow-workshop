import logging

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.dummy import DummyOperator

default_args = {
    "owner": "Maxine",
    "start_date": timezone.datetime(2021, 9, 27),
}
with DAG(
    "my_dag_hw1_1",
    schedule_interval="*/10 * * * *",
    default_args=default_args,
    catchup=False,
    tags=["saksiam"],
) as dag:
    task1 = DummyOperator(task_id="t1")
    task2 = DummyOperator(task_id="t2")
    task3 = DummyOperator(task_id="t3")
    task4 = DummyOperator(task_id="t4")
    task5 = DummyOperator(task_id="t5")
    task6 = DummyOperator(task_id="t6")
    task7 = DummyOperator(task_id="t7")
    task8 = DummyOperator(task_id="t8")
    task9 = DummyOperator(task_id="t9")

    # start >> echo_hello >> say_hello >> end
    task1 >> [task2, task5]
    task2 >> [task3, task6]
    task3 >> task4 >> task9
    task6 >> task8 >> task9
    task5 >> [task6, task7]
    task7 >> task8 >> task9
