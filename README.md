# Data Engineering Nanodegree
## Project 05 - Data Pipelines

## Introduction

A music streaming company, Sparkify, has decided that it is time to introduce
more automation and monitoring to their data warehouse ETL pipelines and come
to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high
grade data pipelines that are dynamic and built from reusable tasks, can be
monitored, and allow easy backfills. They have also noted that the data quality
plays a big part when analyses are executed on top the data warehouse and want
to run tests against their datasets after the ETL steps have been executed to
catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data
warehouse in Amazon Redshift. The source datasets consist of JSON logs that
tell about user activity in the application and JSON metadata about the songs
the users listen to.

## Project Description

This project will introduce you to the core concepts of Apache Airflow. To
complete the project, you will need to create your own custom operators to
perform tasks such as staging the data, filling the data warehouse, and running
checks on the data as the final step.

We have provided you with a project template that takes care of all the imports
and provides four empty operators that need to be implemented into functional
pieces of a data pipeline. The template also contains a set of tasks that need
to be linked to achieve a coherent and sensible data flow within the pipeline.

You'll be provided with a helpers class that contains all the SQL
transformations. Thus, you won't need to write the ETL yourselves, but you'll
need to execute it with your custom operators.

## Requirements

An active AWS Redshift cluster is required to run this Pipeline via Airflow.

## Job Execution

  1. Launch an Airflow instance
  2. Create a connection named `redshift` in
     airflow containing credentials to access the Redshift database instance.
  3. Create a connection named `aws_credentials` in airflow containing credentials
     to the aws account.
  4. Launch the `udac_example_dag` DAG.

## Data Movement

As the DAG is processed, the Sparkify data is moved from it's source in S3 to
staging tables in the redshift cluster.  Then, the staged data is used to
create and load facts and dimensions tables as defined below.

## DAG Tasks

The following tasks are part of the `udac_example_dag`:

  * `start_operator` - dummy operator to signify the start of the DAG.
  * `create_tables_task` - uses the `create_tables.sql` script to create all required database tables
  * `stage_events_to_redshift` - copy events data from source S3 bucket to events staging table
  * `stage_songs_to_redshift` - copy songs data from sources S3 bucket to songs  staging table.
  * `load_songplays_table` - insert song plays facts data to songplays table.
  * `load_user_dimension_table` - insert user dimension data into user table.
  * `load_song_dimension_table` - insert song dimension data into song table.
  * `load_artist_dimension_table` - insert artist dimension data into artist table.
  * `load_time_dimension_table` - insert time dimension data into time table.
  * `run_quality_checks` - check each table for existance of data as a quality check.
  * `end_operator` - dummy operator to signify the end of the DAG.

#### Fact Table

  1. `songplays` - records in log data associated with song plays i.e. records
     with page `NextSong` * _songplay_id, start_time, user_id, level, song_id,
     artist_id, session_id, location, user_agent_

#### Dimension Tables

  2. `users` - users in the app * _user_id, first_name, last_name, gender,
     level_

  3. `songs` - songs in music database * _song_id, title, artist_id, year,
     duration_

  4. `artists` - artists in music database * _artist_id, name, location,
     lattitude, longitude_

  5. `time` - timestamps of records in songplays broken down into specific
     units * _start_time, hour, day, week, month, year, weekday_
