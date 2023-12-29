from pyflink.common import SimpleStringSchema
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.common import Time
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import MapFunction
from pyflink.common import Configuration
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import AggregateFunction, AllWindowFunction



def python_data_stream_example():
    config = Configuration()
    config.set_string("state.checkpoint-storage", "filesystem")
    config.set_string("state.checkpoints.dir", "file:///opt/pyflink/tmp/checkpoints/logs")

    env = StreamExecutionEnvironment.get_execution_environment(config)
    # Set the parallelism to be one to make sure that all data including fired timer and normal data
    # are processed by the same worker and the collected result would be in order which is good for
    # assertion.
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    env.enable_checkpointing(1000)

    type_info: RowTypeInfo = Types.ROW_NAMED(['device_id', 'temperature', 'execution_time'],
                                             [Types.LONG(), Types.DOUBLE(), Types.INT()])

    json_row_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()

    source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('nvdenisov-hse-2023') \
        .set_group_id('pyflink-e2e-source') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(json_row_schema) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    ds = ds.window_all(TumblingProcessingTimeWindows.of(Time.seconds(5))) \
        .apply(MaxWindowFunction)


    sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
                               .set_topic('nvdenisov-hse-2023-processed')
                               .set_value_serialization_schema(SimpleStringSchema())
                               .build()
                               ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    ds.map(IdFunction(), Types.STRING()) \
        .sink_to(sink)
    env.execute_async("Devices preprocessing")


class MaxAggregateFunction(AggregateFunction):

    def create_accumulator(self):
        return float('-inf')

    def add(self, value, accumulator):
        return max(value, accumulator)

    def get_result(self, accumulator):
        return accumulator

    def merge(self, a, b):
        return max(a, b)

    def get_accumulator_type(self):
        return Types.FLOAT()

    def get_result_type(self):
        return Types.FLOAT()

class MaxWindowFunction(AllWindowFunction):

    def apply(self, window, vals, out):
        max_value = float('-inf')
        for val in vals:
            max_value = max(max_value, val[1])
        window_start = window.start
        window_end = window.end
        out.collect((window_start, window_end, max_value))




class IdFunction(MapFunction):

    def map(self, value):
        return value
        # device_id, temperature, execution_time = value
        # return str({"device_id": device_id, "temperature": temperature - 273, "execution_time": execution_time})


if __name__ == '__main__':
    python_data_stream_example()
