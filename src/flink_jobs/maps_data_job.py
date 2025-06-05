import os
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.common import Configuration


# Create the TableEnvironment with the specified configuration
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(environment_settings=env_settings)

# Define the directory containing your connector JARs
jar_dir = "/opt/flink/lib"

# Dynamically add all JARs from the specified directory
jar_files = [
    f"file://{os.path.join(jar_dir, f)}"
    for f in os.listdir(jar_dir)
    if f.endswith(".jar")
]
print(jar_files)
# Apply the configuration to the TableEnvironment
t_env.get_config().set("pipeline.jars", ";".join(jar_files))

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
        'connector' = 'kafka',
        'topic' = 'input_topic',
        'properties.bootstrap.servers' = 'broker:9092',
        'properties.group.id' = 'flink_consumer',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.timestamp-format.standard' = 'ISO-8601'
    )
""")

# Define the Kafka sink table
t_env.execute_sql("""
    CREATE TABLE output_topic (
        `Latitude` DOUBLE,
        `Longitude` DOUBLE,
        `Value` DOUBLE,
        `Unit` STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'output_topic',
        'properties.bootstrap.servers' = 'broker:9092',
        'format' = 'json'
    )
""")

# Insert data into the output topic
t_env.execute_sql("""
    INSERT INTO output_topic
    SELECT `Latitude`, `Longitude`, `Value`, `Unit`
    FROM datasource
""")
