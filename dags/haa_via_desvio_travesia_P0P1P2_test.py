from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import BranchPythonOperator
from providers.stratio.rocket.operators.rocket_operator import RocketOperator
from datetime import datetime

execution_date = datetime.now().strftime('%Y%m%d')

def run_rocket_operator(wf_title, wf_name, wf_path, wf_size, trigger_rule="all_success"):
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
        trigger_rule=trigger_rule
    )
    return task

with DAG(
    'haa_via_desvio_travesia_P0P1P2_test',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["stratio"],
    max_active_tasks=8,
    params={
        "entrenar": Param("False", type="string"),
        "comparar": Param("False", type="string"),
    },
) as dag:

    # Etapa 1
    raw_to_interim_elementos_auscultaciones = run_rocket_operator("raw_to_interim_elementos_auscultaciones", "01-raw-to-interim-elementos-auscultaciones", "/home/haa/01-pipelines/p0-raw-to-interim/01-via/02-via-desvio-travesia", "SM")

    # Etapa 2
    raw_to_interim_averias = run_rocket_operator("raw_to_interim_averias", "02-raw-to-interim-averias", "/home/haa/01-pipelines/p0-raw-to-interim/01-via/02-via-desvio-travesia", "SM")

    # Etapa 3 — paralelo
    extratables_ausc_geom = run_rocket_operator("extratables_ausc_geom", "00-extratables-ausc-geom", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables", "SM")
    extratables_ausc_ultra = run_rocket_operator("extratables_ausc_ultra", "01-extratables-ausc-ultra", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables", "SM")
    extratables_carrilcruzamiento = run_rocket_operator("extratables_carrilcruzamiento", "02-extratables-carrilcruzamiento", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables", "SM")
    extratables_carrilintermedio = run_rocket_operator("extratables_carrilintermedio", "03-extratables-carrilintermedio", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables", "SM")
    extratables_contracarril = run_rocket_operator("extratables_contracarril", "04-extratables-contracarril", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables", "SM")
    extratables_corazon = run_rocket_operator("extratables_corazon", "05-extratables-corazon", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables", "SM")
    extratables_marmita = run_rocket_operator("extratables_marmita", "06-extratables-marmita", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables", "SM")
    extratables_semicambio = run_rocket_operator("extratables_semicambio", "07-extratables-semicambio", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/01-extra-tables", "SM")

    # Etapa 4
    weather_matching = run_rocket_operator("weather_matching", "00-weather-matching", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/02-main", "SM")

    # Etapa 5
    processing_to_master_table_01 = run_rocket_operator("processing_to_master_table_01", "01-processing-to-master-table-01", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/02-main", "SM")

    # Etapa 6
    processing_to_master_table_02 = run_rocket_operator("processing_to_master_table_02", "01-processing-to-master-table-02", "/home/haa/01-pipelines/p1-interim-to-processed/01-via/02-via-desvio-travesia/02-main", "SM")

    # Etapa 7 — paralelo
    def _branch_pipeline_training(**ctx):
        params = ctx.get('params', {})
        return 'pipeline_training' if eval("entrenar", {}, params) else 'preprocessing_prediction'
    check_pipeline_training = BranchPythonOperator(
        task_id="check_pipeline_training",
        python_callable=_branch_pipeline_training,
        dag=dag,
    )
    pipeline_training = run_rocket_operator("pipeline_training", "01-pipeline-training", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/01-train", "SM")
    check_pipeline_training >> pipeline_training
    preprocessing_prediction = run_rocket_operator("preprocessing_prediction", "00-preprocessing-prediction", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/02-prediction", "SM")

    # Etapa 8
    test_train_ml_project = run_rocket_operator("test_train_ml_project", "02-test-train-ml-project", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/01-train", "SM", "none_failed")

    # Etapa 9 — paralelo
    def _branch_model_comparison(**ctx):
        params = ctx.get('params', {})
        return 'model_comparison' if eval("comparar", {}, params) else 'aux_overwrite_model'
    check_model_comparison = BranchPythonOperator(
        task_id="check_model_comparison",
        python_callable=_branch_model_comparison,
        dag=dag,
    )
    model_comparison = run_rocket_operator("model_comparison", "03-Model-comparison", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/01-train", "SM")
    check_model_comparison >> model_comparison
    aux_overwrite_model = run_rocket_operator("aux_overwrite_model", "04-aux-overwrite-model", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/01-train", "SM")

    # Etapa 10
    prediction = run_rocket_operator("prediction", "01-prediction", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/02-prediction", "SM", "none_failed")

    # Etapa 11
    feature_quantiles_matrix = run_rocket_operator("feature_quantiles_matrix", "02-feature-quantiles-matrix", "/home/haa/01-pipelines/p2-modelling/01-via/02-via-desvio-travesia/02-prediction", "SM")
    # Dependencias
    raw_to_interim_elementos_auscultaciones >> raw_to_interim_averias
    raw_to_interim_averias >> [extratables_ausc_geom, extratables_ausc_ultra, extratables_carrilcruzamiento, extratables_carrilintermedio, extratables_contracarril, extratables_corazon, extratables_marmita, extratables_semicambio]
    [extratables_ausc_geom, extratables_ausc_ultra, extratables_carrilcruzamiento, extratables_carrilintermedio, extratables_contracarril, extratables_corazon, extratables_marmita, extratables_semicambio] >> weather_matching
    weather_matching >> processing_to_master_table_01
    processing_to_master_table_01 >> processing_to_master_table_02
    processing_to_master_table_02 >> [check_pipeline_training, preprocessing_prediction]
    [pipeline_training, preprocessing_prediction] >> test_train_ml_project
    test_train_ml_project >> [check_model_comparison, aux_overwrite_model]
    [model_comparison, aux_overwrite_model] >> prediction
    prediction >> feature_quantiles_matrix
    check_pipeline_training >> preprocessing_prediction  # si no: saltar a '00-preprocessing-prediction'
    check_model_comparison >> aux_overwrite_model  # si no: saltar a '04-aux-overwrite-model'
