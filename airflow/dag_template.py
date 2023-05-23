import dlt
from airflow.decorators import dag
from dlt.helpers import AirflowTasks

from pipedrive import pipedrive_source as source


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': 'test@test.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'max_active_runs': 1
}


@dag(
    schedule=None,
    start_date="2021-01-01T00:00:00Z",
    catchup=False,
    default_args=default_args
)
def load_data():
    # store data on the bucket
    tasks = AirflowTasks("pipeline_decomposed", use_data_folder=True)

    p = dlt.pipeline(pipeline_name='pipeline_name',
                     dataset_name='dataset_name',
                     destination='duckdb',
                     full_refresh=False # must be false if we decompose
                     )

    # we keep secrets in `dlt_secrets_toml`, same for bigquery credentials
    tasks.add_run(p, source(), decompose="serialize", trigger_rule="all_done", retries=0, provide_context=True)

load_data()