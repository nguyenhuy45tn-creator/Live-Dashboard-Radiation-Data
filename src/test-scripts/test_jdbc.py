from pyflink.table import (EnvironmentSettings, TableEnvironment, TableDescriptor, Schema, DataTypes, FormatDescriptor)
from pyflink.table.expressions import col, lit
from pyflink.table.window import Slide

t_env = TableEnvironment.create(
    environment_settings=EnvironmentSettings.in_streaming_mode())


t_env.get_config().set("pipeline.jars", "file:///mnt/c/Users/hoang/Documents/MasterStudium/04_semester/BigData/bd25_project_m11_c/flink-1.20.1/lib/flink-connector-jdbc-3.3.0-1.20.jar;file:///mnt/c/Users/hoang/Documents/MasterStudium/04_semester/BigData/bd25_project_m11_c/flink-1.20.1/lib/postgresql-42.7.6.jar")

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
t_env.execute_sql(source_ddl).wait()

operation_dml = """ INSERT INTO test2 VALUES (('e23'), ('43f'), ('34v'));"""

t_env.execute_sql(operation_dml).wait()