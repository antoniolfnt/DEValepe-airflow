from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlalchemy

path_temp_csv = "/tmp/dataset.csv"
email_failed = "felipesf05@gmail.com"

dag = DAG(
    dag_id="elt-pipeline1",
    description="Pipeline para o processo de ETL dos ambientes de produção oltp ao olap.",
    start_date=days_ago(2),
    schedule_interval=None,
)

def _extract():
    #conectando a base de dados de oltp.
    engine_mysql_oltp = sqlalchemy.create_engine('mysql+pymysql://root:airflow@172.17.0.2:3306/employees')
    
    #selecionando os dados.
    dataset_df = pd.read_sql_query(r"""
                        SELECT   emp.emp_no
                        , emp.first_name
                        , emp.last_name
                        , sal.salary
                        , titles.title 
                        FROM employees emp 
                        INNER JOIN (SELECT emp_no, MAX(salary) as salary 
                                    FROM salaries GROUP BY emp_no) 
                        sal ON sal.emp_no = emp.emp_no 
                        INNER JOIN titles 
                        ON titles.emp_no = emp.emp_no
                        LIMIT 1000"""
                        ,engine_mysql_oltp
    )
    #exportando os dados para a área de stage.
    dataset_df.to_csv(
        path_temp_csv,
        index=False
    )