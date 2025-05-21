from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes, FormatDescriptor)
from pyflink.table.expressions import col, lit
from pyflink.table.window import Slide

# environment configuration
t_env = TableEnvironment.create(
    environment_settings=EnvironmentSettings.in_batch_mode())

# register Orders table and Result table sink in table environment
source_data_path = "./test_data/test_measurements.csv"
result_data_path = "./out/"
# source_ddl = f"""
#         create table (
#             ts DATETIME,
#             longtitude BIGINT,
#             c BIGINT,
#             rowtime TIMESTAMP(3),
#             WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
#         ) with (
#             'connector' = 'filesystem',
#             'format' = 'csv',
#             'path' = '{source_data_path}'
#         )
#         """

t_env.create_table(
    'datasource',
    TableDescriptor.for_connector('filesystem')
        .schema(Schema.new_builder()
                .column('timestamp', DataTypes.TIMESTAMP())
                .column('longtitude', DataTypes.STRING())
                .column('latitude', DataTypes.STRING())
                .column('measurement', DataTypes.FLOAT())
                .column('Location Name', DataTypes.STRING())
                .column('Device ID', DataTypes.STRING())
                .column('MD5Sum', DataTypes.STRING())
                .column("Height", DataTypes.STRING())
                .column("Surface", DataTypes.STRING())
                .column("Radiation", DataTypes.STRING())
                .column("Uploaded Time", DataTypes.STRING())
                .column("Loader ID",DataTypes.STRING())
                .build())
        .option('path', source_data_path)
        .format('csv')
        .build())

table = t_env.from_path("datasource")

sink_ddl = f"""
    create table Outputs (
        longtitude STRING,
        latitude STRING,
        measurement FLOAT
    ) with (
        'connector' = 'print'
    )
    """
t_env.execute_sql(sink_ddl)

table = (table
        .window(Slide.over(lit(5).seconds)
                .every(lit(2).seconds)
                .on(col("timestamp")).alias("ts"))
        .group_by(col('longtitude'), col('latitude'), col('ts'))
        .select(col('longtitude'), col('latitude'),col('measurement').avg)
        )

table.execute_insert("Outputs").wait()



# specify table program
# orders = t_env.from_path("Orders")  # schema (a, b, c, rowtime)

# orders.group_by(col("a")).select(col("a"), col("b").count.alias('cnt')).execute_insert("result").wait()
