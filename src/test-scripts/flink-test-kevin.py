from pyflink.table import EnvironmentSettings, TableEnvironment

# Set up the Table Environment
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Define the source Kafka table
t_env.execute_sql("""
CREATE TABLE input_topic (
    `Message` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'input_topic',
    'properties.bootstrap.servers' = 'broker:9092',
    'properties.group.id' = 'flink_consumer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
""")

# Define the sink Kafka table
t_env.execute_sql("""
CREATE TABLE output_topic (
    `Message` BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'output_topic',
    'properties.bootstrap.servers' = 'broker:9092',
    'format' = 'json'
)
""")

# Insert data from input_topic to output_topic
t_env.execute_sql("""
INSERT INTO output_topic
SELECT * FROM input_topic
""")
