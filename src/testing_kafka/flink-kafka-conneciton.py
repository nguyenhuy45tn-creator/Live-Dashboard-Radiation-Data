import os
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.common import Configuration
from pathlib import Path


# Create the TableEnvironment with the specified configuration
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(environment_settings=env_settings)

# Define the directory containing your connector JARs
jar_dir = Path(r"jar-files")

jar_files = jar_dir.glob("*.jar")

jar_files_abs_address = []
for file in jar_files:
    jar_files_abs_address.append(f"file://{file.absolute().as_posix()}")

# Dynamically add all JARs from the specified directory
# jar_files = ["file:///mnt/c/Users/hoang/Documents/MasterStudium/04_semester/BigData/bd25_project_m11_c/flink-1.20.1/lib/flink-sql-connector-kafka-3.3.0-1.20.jar",
# "file:///mnt/c/Users/hoang/Documents/MasterStudium/04_semester/BigData/bd25_project_m11_c/flink-1.20.1/lib/flink-connector-jdbc-3.3.0-1.20.jar",
# "file:///mnt/c/Users/hoang/Documents/MasterStudium/04_semester/BigData/bd25_project_m11_c/flink-1.20.1/lib/postgresql-42.7.6.jar"
# ]
print(jar_files)
# Apply the configuration to the TableEnvironment
t_env.get_config().set("pipeline.jars", ";".join(jar_files_abs_address))

# Define the Kafka source table
t_env.execute_sql("""
    CREATE TABLE datasource (
        `Captured Time` TIMESTAMP(3),
        `Latitude` DOUBLE,
        `Longitude` DOUBLE,
        `Value` DOUBLE,
        `Unit` STRING,
        `Location Name` STRING,
        `Device ID` STRING,
        `MD5Sum` STRING,
        `Height` DOUBLE,
        `Surface` STRING,
        `Radiation` STRING,
        `Uploaded Time` STRING,
        `Loader ID` STRING,
        WATERMARK FOR `Captured Time` AS `Captured Time` - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'filesystem',
        'path'= 'file:///mnt/c/Users/hoang/Documents/MasterStudium/04_semester/BigData/bd25_project_m11_c/test_data/test_measurements.csv',
        'format'='csv'
    )
""")

# Define the Kafka sink table
# t_env.execute_sql("""
#     CREATE TABLE output_topic (
#         `Latitude` DOUBLE,
#         `Longitude` DOUBLE,
#         `Value` DOUBLE,
#         `Unit` STRING,
#         `timestamp`
    # ) WITH (
    #     'connector' = 'kafka',
    #     'topic' = 'output_topic',
    #     'properties.bootstrap.servers' = 'localhost:29092',
    #     'format' = 'json'
#     )
# """)

# Insert data into the output topic
t_env.execute_sql("""
    CREATE TABLE output_topic
    WITH (
        'connector' = 'kafka',
        'topic' = 'output_topic',
        'properties.bootstrap.servers' = 'localhost:29092',
        'format' = 'json'
    )
    AS SELECT `Latitude`, `Longitude`, `Value`, `Unit`, `Captured Time` AS `timestamp`
    FROM datasource
""").wait()
