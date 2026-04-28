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
    p0_01_raw_to_interim_elementos_auscultaciones = run_rocket_operator("p0_01_raw_to_interim_elementos_auscultaciones", "P0_01-raw-to-interim-elementos-auscultaciones", "/home/haa/01-pipelines/p0-raw-to-interim/01-via/02-via-desvio-travesia/01-raw-to-interim-elementos-auscultaciones", "XS")

    # Etapa 2
    p0_02_raw_to_interim_averias = run_rocket_operator("p0_02_raw_to_interim_averias", "P0_02-raw-to-interim-averias", "/home/haa/01-pipelines/p0-raw-to-interim/01-via/02-via-desvio-travesia/02-raw-to-interim-averias", "XS")

    # Etapa 3
    p1_00_extratables_ausc_geom = run_rocket_operator("p1_00_extratables_ausc_geom", "P1_00-extratables-ausc-geom", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/00-extratables-ausc-geom", "XS")

    # Etapa 4
    p1_01_extratables_ausc_ultra = run_rocket_operator("p1_01_extratables_ausc_ultra", "P1_01-extratables-ausc-ultra", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/01-extratables-ausc-ultra", "XS")

    # Etapa 5
    p1_02_extratables_carrilcruzamiento = run_rocket_operator("p1_02_extratables_carrilcruzamiento", "P1_02-extratables-carrilcruzamiento", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/02-extratables-carrilcruzamiento", "XS")

    # Etapa 6
    p1_03_extratables_carrilintermedio = run_rocket_operator("p1_03_extratables_carrilintermedio", "P1_03-extratables-carrilintermedio", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/03-extratables-carrilintermedio", "XS")

    # Etapa 7
    p1_04_extratables_contracarril = run_rocket_operator("p1_04_extratables_contracarril", "P1_04-extratables-contracarril", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/04-extratables-contracarril", "XS")

    # Etapa 8
    p1_05_extratables_corazon = run_rocket_operator("p1_05_extratables_corazon", "P1_05-extratables-corazon", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/05-extratables-corazon", "XS")

    # Etapa 9
    p1_06_extratables_marmita = run_rocket_operator("p1_06_extratables_marmita", "P1_06-extratables-marmita", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/06-extratables-marmita", "XS")

    # Etapa 10
    p1_07_extratables_semicambio = run_rocket_operator("p1_07_extratables_semicambio", "P1_07-extratables-semicambio", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables/07-extratables-semicambio", "XS")

    # Etapa 11
    p1_00_weather_matching = run_rocket_operator("p1_00_weather_matching", "P1_00-weather-matching", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/02-main/00-weather-matching", "XS")

    # Etapa 12
    p1_01_processing_to_master_table_01 = run_rocket_operator("p1_01_processing_to_master_table_01", "P1_01-processing-to-master-table-01", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/02-main/01-processing-to-master-table-01", "XS")

    # Etapa 13
    p1_01_processing_to_master_table_02 = run_rocket_operator("p1_01_processing_to_master_table_02", "P1_01-processing-to-master-table-02", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/02-main/01-processing-to-master-table-02", "XS")

    # Etapa 14
    p2_01_pipeline_training = run_rocket_operator("p2_01_pipeline_training", "P2_01-pipeline-training", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/01-train/01-pipeline-training", "XS")

    # Etapa 15
    p2_03_test_train_ml_project = run_rocket_operator("p2_03_test_train_ml_project", "P2_03-test-train-ml-project", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/01-train/03-test-train-ml-project", "XS")

    # Etapa 16
    p2_05_model_comparison = run_rocket_operator("p2_05_model_comparison", "P2_05-Model-comparison", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/01-train/05-Model-comparison", "XS")

    # Etapa 17
    p2_06_aux_overwrite_model = run_rocket_operator("p2_06_aux_overwrite_model", "P2_06-aux-overwrite-model", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/01-train/06-aux-overwrite-model", "XS")

    # Etapa 18
    p2_00_preprocessing_prediction = run_rocket_operator("p2_00_preprocessing_prediction", "P2_00-preprocessing-prediction", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/02-prediction/00-preprocessing-prediction", "XS")

    # Etapa 19
    p2_01_prediction = run_rocket_operator("p2_01_prediction", "P2_01-prediction", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/02-prediction/01-prediction", "XS")

    # Etapa 20
    p2_02_feature_quantiles_matrix = run_rocket_operator("p2_02_feature_quantiles_matrix", "P2_02-feature-quantiles-matrix", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/02-prediction/02-feature-quantiles-matrix", "XS")
    # Dependencias
    p0_01_raw_to_interim_elementos_auscultaciones >> p0_02_raw_to_interim_averias
    p0_02_raw_to_interim_averias >> p1_00_extratables_ausc_geom
    p1_00_extratables_ausc_geom >> p1_01_extratables_ausc_ultra
    p1_01_extratables_ausc_ultra >> p1_02_extratables_carrilcruzamiento
    p1_02_extratables_carrilcruzamiento >> p1_03_extratables_carrilintermedio
    p1_03_extratables_carrilintermedio >> p1_04_extratables_contracarril
    p1_04_extratables_contracarril >> p1_05_extratables_corazon
    p1_05_extratables_corazon >> p1_06_extratables_marmita
    p1_06_extratables_marmita >> p1_07_extratables_semicambio
    p1_07_extratables_semicambio >> p1_00_weather_matching
    p1_00_weather_matching >> p1_01_processing_to_master_table_01
    p1_01_processing_to_master_table_01 >> p1_01_processing_to_master_table_02
    p1_01_processing_to_master_table_02 >> p2_01_pipeline_training
    p2_01_pipeline_training >> p2_03_test_train_ml_project
    p2_03_test_train_ml_project >> p2_05_model_comparison
    p2_05_model_comparison >> p2_06_aux_overwrite_model
    p2_06_aux_overwrite_model >> p2_00_preprocessing_prediction
    p2_00_preprocessing_prediction >> p2_01_prediction
    p2_01_prediction >> p2_02_feature_quantiles_matrix
