# Import libraries
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.decorators import task
import requests
import bs4
from bs4 import BeautifulSoup
import pprint
from pathlib import Path
import os, json

# Create DAG

args = {"owner": "Mr.G"}

with DAG(
    dag_id= "ETL_WebtoSQL",
    template_searchpath= ['dags/data/'],
    default_args= args,
    catchup= True,
    start_date= datetime(2024,4,20), ## the start day can be the day you want
    max_active_runs= 1,
    schedule_interval=timedelta(days=1)
    
) as dag: 
    Begin = EmptyOperator(
        task_id="Begin"
    )
    
    def create_custom_hn():
        url1 = "https://news.ycombinator.com/news"
        res = requests.get(url= url1)
        soup = BeautifulSoup(res.text, "html.parser")

        links = soup.select(".titleline > a")
        votes = soup.select(".subtext")
        hn = []
        dict1 = {"keys":[]}
        
        for index, item in enumerate(links):
            title = links[index].getText()
            href = links[index].get('href', None)
            vote = votes[index].select('.score')
            # ds = "{{ ds }}"
            if len(vote) >0:
                point = int((vote[0].text).replace(" points", ""))
                if point > 99:
                    hn.append({'title': title, 'link': href, "vote": point})
        return hn
    
    def create_sql():
        with open(f'dags/data/ETL_webtosqld.sql', "w") as writer:
            for line in list(sorted(create_custom_hn(), key= lambda k: k['vote'], reverse= True)):
                if "'" in line["title"]:
                    title = line["title"].replace("'", "''")
                else: 
                    title = line['title']
            
                if "'" in line["link"]:
                    link = line["link"].replace("'", "''")
                else:
                    link = line['link']

                vote = line["vote"]
                date_get = '{{ds}}'
                writer.write(f"INSERT INTO etl_webtosql VALUES ('{title}', '{link}', {vote}, '{date_get}');" "\n")
        return "SQL file completed successfully"

    Get_data = PythonOperator(
        task_id = "get_data",
        python_callable = create_custom_hn
    
    )
    
    Create_sql_file = PythonOperator(
        task_id = 'Create_sql_file',
        python_callable= create_sql
    )

    Create_table = PostgresOperator(
        task_id = "Create_ETL_webtosql_table",
        postgres_conn_id= "postgres_localhost",
        sql = """
            create table if not exists etl_webtosql(
            title text not null,
            link text not null,
            vote int not null,
            execution_date varchar(25) not null,
            date_created DATE NOT NULL DEFAULT CURRENT_DATE
            )
            """
    )

    insert_data = PostgresOperator(
        task_id = "insert_data_to_table",
        postgres_conn_id= "postgres_localhost",
        sql= "ETL_webtosqld.sql"
    )


    Inform = BashOperator(
        task_id = "inform",
        bash_command= 'echo -n "Finish scraping" '
    )


Begin >> Get_data >> Create_sql_file >> Create_table >> insert_data >> Inform
