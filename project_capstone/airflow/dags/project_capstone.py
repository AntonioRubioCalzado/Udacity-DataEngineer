from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'Antonio Rubio',
    'start_date': datetime(2016, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('project_capstone_dend_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 0 1 * *',
          catchup=False
        )


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
        CREATE TABLE IF NOT EXISTS public.i94 (
            cicid int4 NOT NULL,
            i94yr	int4,
            i94mon int4,
            i94cit int4,
            i94res int4,
            i94port varchar(256),
            arrdate varchar(256),
            i94mode varchar(256),
            depdate varchar(256),
            i94visa int4,
            gender varchar(256),
            airline varchar(256),
            airport_name varchar(256),
            state_code varchar(256),
            born_country varchar(256),
            residence_country varchar(256),
            mode varchar(256),
            visa varchar(256)
        );
        CREATE TABLE IF NOT EXISTS public.date (
            date varchar(256),
            day int4,
            month int4,
            year int4,
            week int4,
            integer int4,
            yearday int4
        );
        CREATE TABLE IF NOT EXISTS public.us_states_temperature (
            state varchar(256),
            average_temperature numeric(18,0)
        );
        CREATE TABLE IF NOT EXISTS public.world_temperature (
            country varchar(256),
            average_temperature numeric(18,0)
        );
        CREATE TABLE IF NOT EXISTS public.airport (
            name varchar(256),
            iso_country varchar(256),
            municipality varchar(256),
            iata_code varchar(256),
            state varchar(256)       
        );
        CREATE TABLE IF NOT EXISTS public.demographic (
            state varchar(256),
            state_code	varchar(256),
            white varchar(256),
            total_population varchar(256),
            female_population varchar(256),
            hispanic_latino varchar(256),
            black_or_africanamerican varchar(256),
            median_age varchar(256),
            asian varchar(256),
            number_of_veterans varchar(256),
            american_indian_and_alaska_native varchar(256),
            foreignborn varchar(256),
            male_population varchar(256),
            average_household_size varchar(256)
        );
    """
)

stage_i94_to_redshift = StageToRedshiftOperator(
    task_id='Stage_i94_fact_table',
    dag=dag,
    table="i94",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="arc-udacity-dataengineer-project-capstone",
    s3_key="i94"
)

stage_date_to_redshift = StageToRedshiftOperator(
    task_id='Stage_date_dim_table',
    dag=dag,
    table="date",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="arc-udacity-dataengineer-project-capstone",
    s3_key="date"
)

stage_us_states_temperature_to_redshift = StageToRedshiftOperator(
    task_id='Stage_us_states_temperature_dim_table',
    dag=dag,
    table="us_states_temperature",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="arc-udacity-dataengineer-project-capstone",
    s3_key="us_states_temperature"
)

stage_world_temperature_to_redshift = StageToRedshiftOperator(
    task_id='Stage_world_temperature_dim_table',
    dag=dag,
    table="world_temperature",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="arc-udacity-dataengineer-project-capstone",
    s3_key="world_temperature"
)

stage_airport_to_redshift = StageToRedshiftOperator(
    task_id='Stage_airport_dim_table',
    dag=dag,
    table="airport",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="arc-udacity-dataengineer-project-capstone",
    s3_key="airport",
    delimiter=";"
)

stage_demography_to_redshift = StageToRedshiftOperator(
    task_id='Stage_demography_dim_table',
    dag=dag,
    table="demographic",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="arc-udacity-dataengineer-project-capstone",
    s3_key="demographic"
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=['public.i94', 'public.date', 'public.us_states_temperature', 'public.world_temperature', 'public.airport', 'public.demographic']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Now we define the tasks order in the DAG.

start_operator >> create_tables

create_tables >> stage_i94_to_redshift

stage_i94_to_redshift >> stage_date_to_redshift
stage_i94_to_redshift >> stage_us_states_temperature_to_redshift
stage_i94_to_redshift >> stage_world_temperature_to_redshift
stage_i94_to_redshift >> stage_airport_to_redshift
stage_i94_to_redshift >> stage_demography_to_redshift

stage_date_to_redshift >> run_quality_checks
stage_us_states_temperature_to_redshift >> run_quality_checks
stage_world_temperature_to_redshift >> run_quality_checks
stage_airport_to_redshift >> run_quality_checks
stage_demography_to_redshift >> run_quality_checks
stage_i94_to_redshift >> run_quality_checks

run_quality_checks >> end_operator