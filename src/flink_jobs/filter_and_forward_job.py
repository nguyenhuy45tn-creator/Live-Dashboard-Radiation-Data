import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    EnvironmentSettings, StreamTableEnvironment,
    Schema, TableDescriptor, FormatDescriptor
)
from pyflink.common import Types
from pyflink.common.types import Row as PyRow
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer
)

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    # Add Kafka + JSON JARs
    for jar in [
        "file:///opt/flink/lib/flink-connector-kafka-1.17.2.jar",
        "file:///opt/flink/lib/kafka-clients-3.1.0.jar",
        "file:///opt/flink/lib/flink-sql-connector-kafka-1.17.2.jar",
        "file:///opt/flink/lib/flink-json-1.17.2.jar"
    ]:
        env.add_jars(jar)

    src = KafkaSource.builder() \
        .set_bootstrap_servers("broker:9092") \
        .set_topics("input_topic") \
        .set_group_id("flink-radiation-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    ds = env.from_source(src, WatermarkStrategy.no_watermarks(), "Kafka Src")

    structured = ds.map(
        lambda s: PyRow(
            CapturedTime=json.loads(s).get("Captured Time"),
            Latitude=float(json.loads(s).get("Latitude") or 0.0),
            Longitude=float(json.loads(s).get("Longitude") or 0.0),
            Value=float(json.loads(s).get("Value") or 0.0),
            Unit=json.loads(s).get("Unit")
        ),
        output_type=Types.ROW_NAMED(
            ["CapturedTime","Latitude","Longitude","Value","Unit"],
            [Types.STRING(), Types.DOUBLE(), Types.DOUBLE(), Types.DOUBLE(), Types.STRING()]
        )
    )

    table = t_env.from_data_stream(
        structured,
        Schema.new_builder()
            .column("CapturedTime", "STRING")
            .column("Latitude", "DOUBLE")
            .column("Longitude", "DOUBLE")
            .column("Value", "DOUBLE")
            .column("Unit", "STRING")
            .build()
    )
    t_env.create_temporary_view("RadiationTable", table)

    t_env.create_temporary_table(
    "kafka_json_sink",
    TableDescriptor.for_connector("kafka")
        .schema(
            Schema.new_builder()
                .column("CapturedTime", "STRING")
                .column("Latitude", "DOUBLE")
                .column("Longitude", "DOUBLE")
                .column("Value", "DOUBLE")
                .column("Unit", "STRING")
                .build()
        )
        .format(FormatDescriptor.for_format("json").build())
        .option("properties.bootstrap.servers", "broker:9092")
        .option("topic", "output_topic")
        .option("scan.startup.mode", "earliest-offset")
        .build()
)


    t_env.execute_sql("""
        INSERT INTO kafka_json_sink
        SELECT `CapturedTime`, `Latitude`, `Longitude`, `Value`, `Unit`
        FROM RadiationTable
    """)

    env.execute("Kafka Table JSON Sink")

if __name__ == "__main__":
    main()
