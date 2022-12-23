from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator, PythonVirtualenvOperator, \
    ExternalPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


def x():
    import os
    print("i am call, i am x")
    os.system("python /home/saranoor/Data/spark_project/twitter_sentiment_analysis/twitter_connection.py")


with DAG("spark_dag", start_date=datetime(2022, 12, 13),
         schedule_interval="@daily", catchup=False) as dag:
    # task_twitter = BashOperator(
    #     task_id="run_twitter_api",
    #     bash_command="/home/saranoor/Data/spark_project/venv/bin/python /home/saranoor/Data/spark_project/twitter_sentiment_analysis/twitter_connection.py"
    # )

    virtual_classic = PythonVirtualenvOperator(
        task_id="virtualenv_classic",
        requirements="tweepy",
        system_site_packages=False,
        python_callable=x,
    )

    # call_script = BashOperator(
    #     task_id='call_script',
    #     bash_command='cd /home/saranoor/Data/spark_project;'
    #                  'source /home/saranoor/Data/spark_project/.venv/bin/activate;'
    #                  'python twitter_sentiment_analysis/twitter_connection.py',
    #     retries=2)
    # external_classic = ExternalPythonOperator(
    #     task_id="external_python_classic",
    #     python="/home/saranoor/Data/spark_project/venv/bin/python3",
    #     python_callable=x
    # )
