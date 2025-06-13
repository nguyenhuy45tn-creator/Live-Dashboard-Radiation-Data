from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes, FormatDescriptor)
from pyflink.table.expressions import col, lit
from pyflink.table.window import Slide
from pathlib import Path

# environment configuration
t_env = TableEnvironment.create(
    environment_settings=EnvironmentSettings.in_streaming_mode())

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

# register Orders table and Result table sink in table environment
source_data_path = "./test_data/test_measurements.csv"
result_data_path = "./out/"



t_env.execute_sql(f"""
    CREATE TABLE datasource (
        `Captured Time` TIMESTAMP(3),
        `Latitude` VARCHAR(20),
        `Longitude` VARCHAR(20),
        `Value` DOUBLE,
        `Unit` STRING NULL,
        `Location Name` STRING NULL,
        `Device ID` STRING NULL,
        `MD5Sum` STRING NULL,
        `Height` STRING NULL,
        `Surface` STRING NULL,
        `Radiation` STRING NULL,
        `Uploaded Time` STRING NULL,
        `Loader ID` STRING NULL,
        WATERMARK FOR `Captured Time` AS `Captured Time` - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'safecast_data_1',
        'properties.bootstrap.servers' = 'localhost:29092',
        'properties.group.id' = 'default-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'csv'
    )
""")

t_env.create_table(
    'watermarked_data',
    TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('timestamp', DataTypes.TIMESTAMP(3))
                .column('longtitude', DataTypes.VARCHAR(20))
                .column('latitude', DataTypes.VARCHAR(20))
                .column('measurement', DataTypes.FLOAT())
                .watermark('timestamp', "`timestamp` - INTERVAL '20' SECOND") 
                .build())
        .option('path', "./output/watermarked_data")
        .format('csv')
        .build())



t_env.execute_sql("""
INSERT INTO watermarked_data
SELECT CAST(`Captured Time` AS TIMESTAMP(3)) AS `timestamp`, 
        `Longitude`, 
        `Latitude`, 
        CAST(`Value` AS FLOAT) as measurement
FROM datasource;
""").wait()

table = t_env.from_path('watermarked_data')

sink_w_timestamp = f"""
    create table Outputs_w_timestamp (
        windows_start TIMESTAMP,
        window_end TIMESTAMP,
        longtitude STRING,
        latitude STRING,
        measurement FLOAT
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '{result_data_path}'
    )
    """
t_env.execute_sql(sink_w_timestamp)

table = (table
        .window(Slide.over(lit(5).seconds)
                .every(lit(2).seconds)
                .on(col("timestamp")).alias("ts"))
        .group_by(col('longtitude'), col('latitude'), col('ts'))
        .select(col('ts').start, col('ts').end, col('longtitude'), col('latitude'),col('measurement').avg)
        )

table.execute_insert("Outputs_w_timestamp").wait()

sink_ddl = f"""
    create table Outputs (
        longtitude VARCHAR(20),
        latitude VARCHAR(20),
        measurement FLOAT,
        PRIMARY KEY (longtitude, latitude) NOT ENFORCED
    ) with (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://localhost:5432/postgres',
        'table-name' = 'geo_data',
        'username' = 'postgres',
        'password' = 'example'
    );
    """
t_env.execute_sql(sink_ddl).wait()

t_env.execute_sql(
    """
    INSERT INTO Outputs
    SELECT longtitude, latitude, measurement
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY (longtitude, latitude) 
        ORDER BY windows_start DESC) AS rownum
        FROM Outputs_w_timestamp
    ) 
    WHERE rownum = 1;""").wait()

sink_ddl = f"""
    create table Print 
    with (
        'connector' = 'print'
    )
    AS
    SELECT longtitude, latitude, measurement
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY (longtitude, latitude) 
        ORDER BY windows_start DESC) AS rownum
        FROM Outputs_w_timestamp
    ) 
    WHERE rownum = 1
    """
t_env.execute_sql(sink_ddl).wait()

table = t_env.from_path("Outputs")

pdf = table.to_pandas()

pdf.to_csv("./output/temp_result/result.csv")




# specify table program
# orders = t_env.from_path("Orders")  # schema (a, b, c, rowtime)

# orders.group_by(col("a")).select(col("a"), col("b").count.alias('cnt')).execute_insert("result").wait()
