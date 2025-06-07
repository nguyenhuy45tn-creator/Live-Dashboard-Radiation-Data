from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes, FormatDescriptor)
from pyflink.table.expressions import col, lit
from pyflink.table.window import Slide

t_env = TableEnvironment.create(
    environment_settings=EnvironmentSettings.in_streaming_mode())

path_connector = "file:///mnt/c/Users/hoang/Documents/MasterStudium/04_semester/BigData/bd25_project_m11_c/flink-1.20.1/lib/flink-connector-jdbc-3.3.0-1.20.jar"
path_driver = "file:///mnt/c/Users/hoang/Documents/MasterStudium/04_semester/BigData/bd25_project_m11_c/flink-2.0.0/lib/postgresql-42.7.6.jar"



path = ";".join([path_connector, path_driver])

t_env.get_config().set("pipeline.jars", path)

source_ddl = f"""
create table test2 (
        longtitude VARCHAR(20),
        latitude VARCHAR(20),
       number VARCHAR(20),
       PRIMARY KEY (longtitude, latitude) NOT ENFORCED
   ) with (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://localhost:5432/postgres',
        'table-name' = 'test_table',
        'username' = 'postgres',
       'password' = 'example'
   );
    """

# source_ddl = f"""
# create table test2 (
#         longtitude VARCHAR(20),
#         latitude VARCHAR(20),
#        measurement FLOAT,
#        PRIMARY KEY (longtitude, latitude) NOT ENFORCED
#    ) with (
#         'connector' = 'jdbc',
#         'url' = 'jdbc:postgresql://localhost:5432/postgres',
#         'table-name' = 'geo_data',
#         'username' = 'postgres',
#        'password' = 'example'
#    );
#     """

t_env.execute_sql(source_ddl).wait()

operation_dml = """ INSERT INTO test2(longtitude, latitude, number) VALUES (('e24'), ('44f'), ('25.46'));"""

t_env.execute_sql(operation_dml).wait()