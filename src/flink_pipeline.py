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
        'properties.bootstrap.servers' = 'broker:9092',
        'properties.group.id' = 'default-group',
        'scan.startup.mode' = 'earliest-offset',
        'properties.auto.offset.reset' = 'earliest',
        'format' = 'csv'
    )
""")
t_env.execute_sql(f"""
create table Outputs (
    longtitude VARCHAR(20),
    latitude VARCHAR(20),
    measurement DOUBLE,
    PRIMARY KEY (longtitude, latitude) NOT ENFORCED
) with (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://db:5432/postgres',
    'table-name' = 'geo_data',
    'username' = 'postgres',
    'password' = 'example'
);
""")

t_env.execute_sql("""
CREATE TABLE print_table (
        `longtitude` VARCHAR(20),
        `latitude` VARCHAR(20),
        `measurement` DOUBLE
    ) with (
        'connector' = 'print'
)
""")

# t_env.create_table(
#     'watermarked_data',
#     TableDescriptor.for_connector('filesystem')
#         .schema(Schema.new_builder()
#                 .column('timestamp', DataTypes.TIMESTAMP(3))
#                 .column('longtitude', DataTypes.VARCHAR(20))
#                 .column('latitude', DataTypes.VARCHAR(20))
#                 .column('measurement', DataTypes.FLOAT())
#                 .watermark('timestamp', "`timestamp` - INTERVAL '5' SECOND") 
#                 .build())
#         .option('path', "out/watermarked_data")
#         .format('csv')
#         .build())



# t_env.execute_sql("""
# INSERT INTO watermarked_data
# SELECT CAST(`Captured Time` AS TIMESTAMP(3)) AS `timestamp`, 
#         `Longitude` AS `longtitude`, 
#         `Latitude` AS `latitude`, 
#         CAST(`Value` AS FLOAT) as measurement
# FROM datasource;
# """)
# table = t_env.from_path('watermarked_data')

# # sink_w_timestamp = f"""
# #     create table Outputs_w_timestamp (
# #         windows_start TIMESTAMP,
# #          TIMESTAMP,
# #         longtitude STRING,
# #         latitude STRING,
# #         measurement FLOAT
# #     ) with (
# #         'connector' = 'filesystem',
# #         'path'='file:///opt/flink/out/output_w_timestamp',
# #         'format'='csv'
# #     )
# #     """

# # t_env.execute_sql(sink_w_timestamp)

# t_env.execute_sql("""
# INSERT INTO print_table
# SELECT *
# FROM watermarked_data;
# """
# )

# t_env.create_table(
#     'Outputs_w_timestamp',
#     TableDescriptor.for_connector('filesystem')
#         .schema(Schema.new_builder()
#                 .column('window_start', DataTypes.TIMESTAMP(3))
#                 .column("window_end", DataTypes.TIMESTAMP(3) )
#                 .column('longtitude', DataTypes.VARCHAR(20))
#                 .column('latitude', DataTypes.VARCHAR(20))
#                 .column('measurement', DataTypes.FLOAT()) 
#                 .build())
#         .option("path","out/output_w_timestamp")
#         .option("format","csv")
#         .build())

# # table = (table
# #         .window(Slide.over(lit(5).seconds)
# #                 .every(lit(2).seconds)
# #                 .on(col("timestamp")).alias("ts"))
# #         .group_by(col('longtitude'), col('latitude'), col('ts'))
# #         .select(col('ts').start, col('ts').end, col('longtitude'), col('latitude'),col('measurement').avg)
# # )
# # table.execute_insert("Outputs_w_timestamp")

# t_env.execute_sql("""
# INSERT INTO Outputs_w_timestamp
# SELECT window_start, window_end, longtitude, latitude, measurement
# FROM TABLE(
#     HOP(
#       DATA => TABLE watermarked_data,
#       TIMECOL => DESCRIPTOR(`timestamp`),
#       SLIDE => INTERVAL '2' SECONDS,
#       SIZE => INTERVAL '5' SECONDS))
# """)

# sink_ddl = f"""
#     create table Outputs (
#         longtitude VARCHAR(20),
#         latitude VARCHAR(20),
#         measurement FLOAT,
#         PRIMARY KEY (longtitude, latitude) NOT ENFORCED
#     ) with (
#         'connector' = 'jdbc',
#         'url' = 'jdbc:postgresql://db:5432/postgres',
#         'table-name' = 'geo_data',
#         'username' = 'postgres',
#         'password' = 'example'
#     );
#     """
# t_env.execute_sql(sink_ddl)

# t_env.execute_sql(
#     """
#     INSERT INTO Outputs
#     SELECT longtitude, latitude, measurement
#     FROM (
#         SELECT *, ROW_NUMBER() OVER (PARTITION BY (longtitude, latitude) 
#         ORDER BY window_start DESC) AS rownum
#         FROM Outputs_w_timestamp
#     ) 
#     WHERE rownum = 1;""")

# sink_ddl = f"""
#     create table Print 
#     with (
#         'connector' = 'print'
#     )
#     AS
#     SELECT longtitude, latitude, measurement
#     FROM (
#         SELECT *, ROW_NUMBER() OVER (PARTITION BY (longtitude, latitude) 
#         ORDER BY window_start DESC) AS rownum
#         FROM Outputs_w_timestamp
#     ) 
#     WHERE rownum = 1
#     """
# t_env.execute_sql(sink_ddl)

t_env.execute_sql("""
INSERT INTO Outputs
SELECT longtitude, latitude, measurement
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY (longtitude, latitude) 
    ORDER BY window_start DESC) AS rownum
    FROM (
        SELECT window_start, window_end, `Longitude` AS longtitude, `Latitude` AS latitude, `Value` AS measurement
        FROM TABLE(
            HOP(
            DATA => TABLE datasource,
            TIMECOL => DESCRIPTOR(`Captured Time`),
            SLIDE => INTERVAL '2' SECONDS,
            SIZE => INTERVAL '5' SECONDS))
    )
) 
WHERE rownum = 1;
"""
)

t_env.execute_sql("""
INSERT INTO print_table
SELECT longtitude, latitude, measurement
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY (longtitude, latitude) 
    ORDER BY window_start DESC) AS rownum
    FROM (
        SELECT window_start, window_end, `Longitude` AS longtitude, `Latitude` AS latitude, `Value` AS measurement
        FROM TABLE(
            HOP(
            DATA => TABLE datasource,
            TIMECOL => DESCRIPTOR(`Captured Time`),
            SLIDE => INTERVAL '2' SECONDS,
            SIZE => INTERVAL '5' SECONDS))
    )
) 
WHERE rownum = 1;
"""
)