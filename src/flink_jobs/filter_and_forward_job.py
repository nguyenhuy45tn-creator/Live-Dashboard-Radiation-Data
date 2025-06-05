from pyflink.datastream import StreamExecutionEnvironment, MapFunction
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Types
import json


class ExtractRelevantFields(MapFunction):
    def map(self, value: str) -> str:
        try:
            data = json.loads(value)
            filtered = {
                "Latitude": data.get("Latitude"),
                "Longitude": data.get("Longitude"),
                "Captured Time": data.get("Captured Time"),
                "Value": data.get("Value"),
                "Unit": data.get("Unit"),
            }
            return json.dumps(filtered)
        except Exception as e:
            # Optionally log or handle errors, fallback to original or empty
            return json.dumps({"error": str(e)})


def main():
    # Create execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)  # Set parallelism to 1 for testing
    
    # Add Kafka connector dependencies
    # Update JAR references to match the environment variables
    env.add_jars("file:///opt/flink/lib/flink-connector-kafka-1.17.2.jar")
    env.add_jars("file:///opt/flink/lib/kafka-clients-3.1.0.jar")
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-1.17.2.jar")
    
    # Create source and sink
    source = KafkaSource.builder() \
        .set_bootstrap_servers("broker:9092") \
        .set_topics("input_topic") \
        .set_group_id("flink-radiation-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers("broker:9092") \
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
            .set_topic("output_topic")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()) \
        .build()
    
    # Create the pipeline
    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    # Extract only necessary fields
    processed_stream = stream.map(ExtractRelevantFields())
    
    # Write to sink
    processed_stream.sink_to(sink)
    
    # Execute the job
    env.execute("Filter and Forward Kafka JSON")

if __name__ == '__main__':
    main()