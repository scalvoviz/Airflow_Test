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

# Pasos secuenciales iniciales (P0)
pre_wfs = [
    ("P0_01_raw_to_interim_elementos_auscultaciones", "01-raw-to-interim-elementos-auscultaciones", "/home/haa/01-pipelines/p0-raw-to-interim/01-via/02-via-desvio-travesia/01-raw-to-interim-elementos-auscultaciones", "SM"),
    ("P0_02_raw_to_interim_averias", "02-raw-to-interim-averias", "/home/haa/01-pipelines/p0-raw-to-interim/01-via/02-via-desvio-travesia/02-raw-to-interim-averias", "SM"),
]

# Pasos en paralelo (extra_tables)
extratables_wfs = [
    ("P1_00_extratables_ausc_geom", "00-extratables-ausc-geom", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/00-extratables-ausc-geom", "SM"),
    ("P1_01_extratables_ausc_ultra", "01-extratables-ausc-ultra", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/01-extratables-ausc-ultra", "SM"),
    ("P1_02_extratables_carrilcruzamiento", "02-extratables-carrilcruzamiento", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/02-extratables-carrilcruzamiento", "SM"),
    ("P1_03_extratables_carrilintermedio", "03-extratables-carrilintermedio", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/03-extratables-carrilintermedio", "SM"),
    ("P1_04_extratables_contracarril", "04-extratables-contracarril", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/04-extratables-contracarril", "SM"),
    ("P1_05_extratables_corazon", "05-extratables-corazon", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/05-extratables-corazon", "SM"),
    ("P1_06_extratables_marmita", "06-extratables-marmita", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/06-extratables-marmita", "SM"),
    ("P1_07_extratables_semicambio", "07-extratables-semicambio", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/07-extratables-semicambio", "SM"),
]

# Pasos secuenciales finales (main + modelling)
post_wfs = [
    ("P1_00_weather_matching", "00-weather-matching", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/02-main/00-weather-matching", "SM"),
    ("P1_01_processing_to_master_table_01", "01-processing-to-master-table-01", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/02-main/01-processing-to-master-table-01", "SM"),
    ("P1_01_processing_to_master_table_02", "01-processing-to-master-table-02", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/02-main/01-processing-to-master-table-02", "SM"),
    ("P2_01_pipeline_training", "01-pipeline-training", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/01-train/01-pipeline-training", "SM"),
    ("P2_03_test_train_ml_project", "03-test-train-ml-project", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/01-train/03-test-train-ml-project", "SM"),
    ("P2_05_model_comparison", "05-Model-comparison", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/01-train/05-Model-comparison", "SM"),
    ("P2_06_aux_overwrite_model", "06-aux-overwrite-model", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/01-train/06-aux-overwrite-model", "SM"),
    ("P2_00_preprocessing_prediction", "00-preprocessing-prediction", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/02-prediction/00-preprocessing-prediction", "SM"),
    ("P2_01_prediction", "01-prediction", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/02-prediction/01-prediction", "SM"),
    ("P2_02_feature_quantiles_matrix", "02-feature-quantiles-matrix", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/02-prediction/02-feature-quantiles-matrix", "SM"),
]

with DAG(
    'haa_via_desvio_travesia',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["stratio"],
    max_active_tasks=8,
) as dag:

    pre_tasks = [run_rocket_operator(*wf) for wf in pre_wfs]
    extratables_tasks = [run_rocket_operator(*wf) for wf in extratables_wfs]
    post_tasks = [run_rocket_operator(*wf) for wf in post_wfs]

    # Secuencial: P0_01 >> P0_02
    for i in range(len(pre_tasks) - 1):
        pre_tasks[i] >> pre_tasks[i + 1]

    # Fan-out: P0_02 >> todos los extra_tables en paralelo
    pre_tasks[-1] >> extratables_tasks

    # Fan-in: todos los extra_tables >> weather_matching
    extratables_tasks >> post_tasks[0]

    # Secuencial: weather_matching >> resto de pasos
    for i in range(len(post_tasks) - 1):
        post_tasks[i] >> post_tasks[i + 1]
