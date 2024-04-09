
from airflow import  DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import random

default_args = {
    'start_date': datetime(2024,3,1),
    'end_date': datetime(2024,4,1)
}

def _choose(**context):
    if random.choice(['OK', 'wait'])=='OK':
        return "Confirmacion_Nasa"
    return "Esperando_Confirmacion"

with DAG (dag_id='11-ProyectoFinal',
          schedule_interval='@daily',
          default_args=default_args,
          max_active_runs=1) as dag:
    
    branching= BranchPythonOperator(task_id="Space_Data",
                                    python_callable=_choose)
    
    Confirmacion_Nasa = BashOperator(task_id='Confirmacion_Nasa',
                            bash_command="echo 'Autorización OK'")
    
    Esperando_Confirmacion = BashOperator(task_id='Esperando_Confirmacion',
                            bash_command="echo 'Esperando Autorización'")
    
    Recolectando_Data_Satelite=BashOperator(task_id="Recolectando_Data_Satelite",
                             bash_command="sleep 10 && touch /tmp/data_satelite.txt")
    
    Esperando_Data_Satelite=FileSensor(task_id="Esperando_Data_Satelite",
                                    filepath="/tmp/data_satelite.txt")
    
    Data_Satelite= BashOperator(task_id="Data_Satelite",
                     bash_command="echo 'Data del Salite dispinible en el Fichero'")
    
    Data_SPACEX = BashOperator(task_id = "Data_SPACEX",
                    bash_command="curl https://api.spacexdata.com/v4/launches/past > /tmp/spacex_{{ds_nodash}}.json")
    
    Enviar_msm_Analistas = EmailOperator(task_id='Enviar_msm_Analistas',
                    to = "michaelbustos.a@gmail.com",
                    subject = "Los Datos finales estan disponibles",
                    html_content = "Los datos finales del satelite y de la API de SpaceX están disponibles",
                    dag = dag)   

    
    branching >> [Confirmacion_Nasa, Esperando_Confirmacion] 
    Confirmacion_Nasa >> Recolectando_Data_Satelite >> Esperando_Data_Satelite >> Data_Satelite
    [Data_Satelite,Data_SPACEX] >> Enviar_msm_Analistas