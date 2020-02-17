# Sparkify DB Warehouse

## Overview
A startup with a music streaming app service wants to analyze the data they have been collecting on 
songs and its user activities on the application. 
The data contains the information on what music its uers listened to and other information on the user, songs and artists.
Throughout this database, the company can have an easy access to the structured data on the needed information. 

## Database schema

In the database, the schema used to organize the data is the star schema. It has a fact table of songplays which contains information on the log data of song plays.
Then, it has 4 dimension tables: users, songs, artists and time.
Through this schema, the database is normalized and organized in such a way that its users can efficiently query the data.

## Files
* 'sql_queries.py' contains code to define SQL queries to create tables in the schema and queries needed for ETL pipeline
* 'create_tables.py' contains python code to create the schema in the AWS Redshift cloud using the queries defined in 'sql_queries.py'
* 'elt.py' contains python code to perform the ETL process: 1) Extracting json data objects in the given S3, 2) Transforming and Loading the data into the staging tables on Redshift, 3) Inserting the right data into the tables in the Scheme in Redshift database


## How to run the codes

Depending on the system, postgreSQL should be installed in the local machine. For Mac users, this link illustrates how to set up PostgreSQL on the machine: https://www.codementor.io/@engineerapart/getting-started-with-postgresql-on-mac-osx-are8jcopb.
After the installation, to run the code, create_table.py needs to be run.

Since this runs on AWS redshift, Redshift cluster on AWS is necessary.
Then, create a file called 'dwh.cfg' and fill in as below:

'''
[CLUSTER]
HOST=<host>
DB_NAME=<db_name>
DB_USER=<db_user>
DB_PASSWORD=<db_password>
DB_PORT=<db_port>

[IAM_ROLE]
ARN=<iam_role_arn>

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'

[AWS]
ACCESS_KEY=<yaccess_key>
SECRET_KEY=<secret_key>
```


For OS, simply type "python create_tables.py" at the terminal. After running the code, etl.py needs to be run by typing "python etl.py"


## Example queries

On the redshift Query editor, after completing above, queries can be run to check the quality of the ETL process.
For example, 

* Get users who listened to certain songs at a particular year and month

```
SELECT  sp.songplay_id,
        u.user_id,
        s.song_id,
        u.last_name,
        a.name,
        s.title
FROM songplays AS sp
        JOIN users   AS u ON (u.user_id = sp.user_id)
        JOIN songs   AS s ON (s.song_id = sp.song_id)
        JOIN time    AS t ON (t.start_time = sp.start_time)
WHERE t.year = '2018' AND t.month = '11'
LIMIT 100;
```

