from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator



default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'retries':3,
    'retry_delay':timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    max_active_runs=3,
    catchup=False,
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift_conn",
        aws_credential_id ="aws_credentials",
        table ="staging_events",
        s3_bucket="karlla-batista",
        #s3_templated_key='data/{{ ds }}/log.json',  # Templated path
        s3_static_key='log-data',                    # Static path
        s3_json_metadatafile =True,                 
        s3_json_path="log_json_path.json"
    )
    
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift_conn",
        aws_credential_id ="aws_credentials",
        table ="staging_songs",
        s3_bucket="karlla-batista",
        s3_static_key='song-data'                    
    )
    
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift_conn"
    )

    
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift_conn",
        insert_mode="truncate-insert",
        table="users" 
    )
    
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift_conn",
        insert_mode="truncate-insert",
        table="songs" 
    )
    
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift_conn",
        insert_mode="truncate-insert",
        table="artists" 
    )
    
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift_conn",
        insert_mode="truncate-insert",
        table="time" 
    )
    
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift_conn",
        params={'tables':['users','songs','artists','time']}
    )
    
    end_operator = DummyOperator(task_id='end_execution')

    
    start_operator >> stage_events_to_redshift 
    start_operator >> stage_songs_to_redshift
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table
    load_songplays_table >> load_user_dimension_table   >>  run_quality_checks
    load_songplays_table >> load_song_dimension_table   >>  run_quality_checks
    load_songplays_table >> load_artist_dimension_table >>  run_quality_checks
    load_songplays_table >> load_time_dimension_table   >>  run_quality_checks
    run_quality_checks >> end_operator
    
    
final_project_dag = final_project()