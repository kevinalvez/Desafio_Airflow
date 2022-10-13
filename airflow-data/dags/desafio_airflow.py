from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import pandas as pd
import sqlite3

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
    # Definição da função py
    def export_output_csv() :
        # Conexão com o banco de dados
        conn = sqlite3.connect('./Northwind_small.sqlite', isolation_level=None,
        detect_types=sqlite3.PARSE_COLNAMES)
        # Consulta da tabela Order
        sql_com = """
            SELECT * FROM "Order"
        """
        # Armazenamento da consulta em um dataframe
        db_df = pd.read_sql_query(sql_com, conn)
        # Criação do arquivo csv com o resultado da consulta
        db_df.to_csv('./output_orders.csv', index=False)

    # PyOperator do export_csv
    export_final_output = PythonOperator(
        task_id='export_sqlite_csv',
        python_callable = export_output_csv,
        provide_context = True
    )

    # Definição da função py    
    def export_output_txt() :
        # Conexão com o banco de dados
        conn = sqlite3.connect('./Northwind_small.sqlite', isolation_level=None,
        detect_types=sqlite3.PARSE_COLNAMES)
        # Consulta da tabela OrderDetail
        sql_com = """
            SELECT * FROM OrderDetail
        """
        # Armazenamento da consulta em um dataframe
        od_df = pd.read_sql_query(sql_com, conn)
        # Armazenamento de dados do csv em um dataframe
        o_df = pd.read_csv('./output_orders.csv')
        # Utilização do "merge" do pandas para cruzar informações com join entre os dados do BD e os dados do CSV
        df_merge = pd.merge(od_df, o_df, how='inner', left_on = 'OrderId', right_on = 'Id')
        # Soma da quantidade de pedidos para a cidade Rio de Janeiro
        df_query = df_merge.query('(ShipCity == "Rio de Janeiro")')['Quantity'].sum()
        # Criação do arquivo txt com o resultado da consulta
        dot_txt = open("./count.txt", "a")
        # Transformação do resultado do dataframe para str
        df_query_string = df_query.astype(str)
        # Preenchimento do arquivo txt com a string
        dot_txt.write(df_query_string)

    # PyOperator do export txt
    export_count_txt = PythonOperator(
        task_id='export_txt',
        python_callable = export_output_txt,
        provide_context = True
    )

    # PyOperator do export final answer
    export_final_answer = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    
    )

    # Ordenação de execução de tarefas
    export_final_output >> export_count_txt >> export_final_answer

