from airflow import DAG
from providers.stratio.rocket.operators.rocket_operator import RocketOperator
from datetime import datetime

execution_date = datetime.now().strftime('%Y%m%d')

def run_rocket_operator(wf_title, wf_name, wf_path, wf_size):
    task = RocketOperator(
        task_id=wf_title,
        connection_id="rocket-connector",
        group_name=wf_path,
        workflow_name=wf_name,
        project_id="82afb56f-e8d7-4436-b851-9e517e0cee73",
        retries_status=999,
        status_polling_frequency=30,
        paramsLists=[wf_size, "Environment", "SparkConfigurations"],
        extra_params=[],
        execution_name=f"airflow_{execution_date}",
        extendedAuditInfo=True,
        dag=dag,
        trigger_rule="all_success"
    )
    return task

with DAG(
    'dag_test',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["stratio"],
    max_active_tasks=1,
) as dag:

    # Etapa 1
    p0_01_raw_to_interim_elementos_auscultaciones = run_rocket_operator("p0_01_raw_to_interim_elementos_auscultaciones", "P0_01-raw-to-interim-elementos-auscultaciones", "/home/haa/01-pipelines/p0-raw-to-interim/01-via/02-via-desvio-travesia/01-raw-to-interim-elementos-auscultaciones", "SM")

    # Etapa 2
    p0_02_raw_to_interim_averias = run_rocket_operator("p0_02_raw_to_interim_averias", "P0_02-raw-to-interim-averias", "/home/haa/01-pipelines/p0-raw-to-interim/01-via/02-via-desvio-travesia/02-raw-to-interim-averias", "SM")

    # Etapa 3
    p1_00_extratables_ausc_geom = run_rocket_operator("p1_00_extratables_ausc_geom", "P1_00-extratables-ausc-geom", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/00-extratables-ausc-geom", "SM")

    # Etapa 4
    p1_01_extratables_ausc_ultra = run_rocket_operator("p1_01_extratables_ausc_ultra", "P1_01-extratables-ausc-ultra", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/01-extratables-ausc-ultra", "SM")

    # Etapa 5
    p1_02_extratables_carrilcruzamiento = run_rocket_operator("p1_02_extratables_carrilcruzamiento", "P1_02-extratables-carrilcruzamiento", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/02-extratables-carrilcruzamiento", "SM")

    # Etapa 6
    p1_03_extratables_carrilintermedio = run_rocket_operator("p1_03_extratables_carrilintermedio", "P1_03-extratables-carrilintermedio", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/03-extratables-carrilintermedio", "SM")
    # Dependencias
    p0_01_raw_to_interim_elementos_auscultaciones >> p0_02_raw_to_interim_averias
    p0_02_raw_to_interim_averias >> p1_00_extratables_ausc_geom
    p1_00_extratables_ausc_geom >> p1_01_extratables_ausc_ultra
    p1_01_extratables_ausc_ultra >> p1_02_extratables_carrilcruzamiento
    p1_02_extratables_carrilcruzamiento >> p1_03_extratables_carrilintermedio
