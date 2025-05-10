import argparse
import logging
import sys

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.BATCH)
env.set_parallelism(1)


ds = env.from_source(
    source=FileSource.for_record_stream_format(StreamFormat.text_line_format(),
                                               "test_data/test_measurements.csv")
                     .process_static_file_set().build(),
    watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
    source_name="file_source"
)

def split(line: str):
    parts = line.split(',')
    result = (parts[1], parts[2], float(parts[3]))
    yield result

# compute word count
ds = ds.flat_map(split,
            output_type=Types.TUPLE([Types.STRING(), 
                                     Types.STRING(), 
                                     Types.LONG()])) \
       .key_by(lambda i: ( i[0], i[1]),
               key_type=Types.TUPLE([ Types.STRING(), 
                                     Types.STRING()])) \
       .reduce(lambda i, j: (i[0], i[1], i[2] + j[2]))

# # compute word count
# ds = ds.flat_map(split, 
#                 output_type=Types.TUPLE([Types.STRING(), 
#                                         Types.STRING(), 
#                                         Types.STRING(), 
#                                         Types.LONG()])) \
#        .key_by(lambda i: (i[0], i[1], i[2])) \
#        .reduce(lambda i, j: (i[0], i[1], i[2], i[3] + j[3]))

ds.sink_to(
    sink=FileSink.for_row_format(
        base_path="temp.txt",
        encoder=Encoder.simple_string_encoder())
    .with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("prefix")
        .with_part_suffix(".ext")
        .build())
    .with_rolling_policy(RollingPolicy.default_rolling_policy())
    .build()
)

env.execute()
