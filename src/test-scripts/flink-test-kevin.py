from pyflink.common import SimpleStringSchema, Types
from pyflink.common.serialization import Encoder
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaOffsetsInitializer, KafkaRecordSerializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

def main():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Define Kafka source
    source = KafkaSource.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_topics("input_topic") \
        .set_group_id("flink_consumer_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Create data stream from Kafka source
    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    # Define processing logic (e.g., convert message to uppercase)
    processed_stream = stream.map(lambda value: value.upper(), output_type=Types.STRING())

    # Define Kafka sink
    sink = KafkaSink.builder() \
        .set_bootstrap_servers("localhost:9092") \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
                .set_topic("output_topic")
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
        ) \
        .build()

    # Add sink to the processed stream
    processed_stream.sink_to(sink)

    # Execute the Flink job
    env.execute("Kafka Stream Processing Job")

if __name__ == '__main__':
    main()
