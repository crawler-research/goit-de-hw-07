from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.state import State
import random
import time
from airflow.utils.dates import days_ago


def set_dag_success(ti, **kwargs):
    dag_run = kwargs["dag_run"]
    dag_run.set_state(State.SUCCESS)


def choose_random_medal():
    return random.choice(["Gold", "Silver", "Bronze"])


def simulate_processing_delay():
    time.sleep(35)


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

mysql_conn_id = "mkuzyshyn"

with DAG(
    "mkuzyshyn_dag2",
    default_args=default_args,
    schedule_interval=None,  
    catchup=False,  # 
    tags=["medal_counting"],
) as dag:

    # Task 1:
    create_table = MySqlOperator(
        task_id="create_medal_table",
        mysql_conn_id=mysql_conn_id,
        sql="""
        CREATE TABLE IF NOT EXISTS neo_data.medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            medal_count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # Task 2:
    select_medal = PythonOperator(
        task_id="select_medal",
        python_callable=choose_random_medal,
    )

    # Task 3: 
    def medal_branching_logic(**kwargs):
        selected_medal = kwargs["ti"].xcom_pull(task_ids="select_medal")
        if selected_medal == "Gold":
            return "count_gold_medals"
        elif selected_medal == "Silver":
            return "count_silver_medals"
        else:
            return "count_bronze_medals"

    branch_task = BranchPythonOperator(
        task_id="branch_on_medal",
        python_callable=medal_branching_logic,
        provide_context=True,
    )

    # Task 4:
    count_bronze = MySqlOperator(
        task_id="count_bronze_medals",
        mysql_conn_id=mysql_conn_id,
        sql="""
           INSERT INTO neo_data.medal_counts (medal_type, medal_count)
           SELECT 'Bronze', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Bronze';
           """,
    )

    count_silver = MySqlOperator(
        task_id="count_silver_medals",
        mysql_conn_id=mysql_conn_id,
        sql="""
           INSERT INTO neo_data.medal_counts (medal_type, medal_count)
           SELECT 'Silver', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Silver';
           """,
    )

    count_gold = MySqlOperator(
        task_id="count_gold_medals",
        mysql_conn_id=mysql_conn_id,
        sql="""
           INSERT INTO neo_data.medal_counts (medal_type, medal_count)
           SELECT 'Gold', COUNT(*)
           FROM olympic_dataset.athlete_event_results
           WHERE medal = 'Gold';
           """,
    )

    delay_task = PythonOperator(
        task_id="simulate_delay",
        python_callable=simulate_processing_delay,
        trigger_rule=TriggerRule.ONE_SUCCESS,  # Runs if at least one task succeeds
    )

    verify_recent_records = SqlSensor(
        task_id="check_recent_records",
        conn_id=mysql_conn_id,
        sql="""
            WITH recent_records AS (
                SELECT COUNT(*) as record_count
                FROM neo_data.medal_counts
                WHERE created_at >= NOW() - INTERVAL 30 SECOND
            )
            SELECT record_count > 0 FROM recent_records;
        """,
        mode="poke",
        poke_interval=10, 
        timeout=30,
    )

    # Dence
    create_table >> select_medal >> branch_task
    (
        branch_task
        >> [count_bronze, count_silver, count_gold]
        >> delay_task
    )
    delay_task >> verify_recent_records