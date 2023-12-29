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
from pyflink.datastream.functions import MapFunction, ReduceFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import AggregateFunction, AllWindowFunction
from pyflink.datastream.window import SlidingProcessingTimeWindows



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


    sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
                               .set_topic('nvdenisov-hse-2023-processed')
                               .set_value_serialization_schema(SimpleStringSchema())
                               .build()
                               ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")

    ds.window_all((SlidingProcessingTimeWindows.of(size=Time.seconds(5), slide=Time.seconds(1)))) \
        .reduce(MaxReducer()) \
        .map(TemperatureFunction(), Types.STRING()) \
        .sink_to(sink)

    env.execute_async("Devices preprocessing")


class MaxReducer(ReduceFunction):

    def reduce(self, v1, v2):
        return max(v1, v2, key=lambda x: x.temperature)


class TemperatureFunction(MapFunction):

    def map(self, value):
        device_id, temperature, execution_time = value
        return str({"device_id": device_id, "temperature": temperature - 273, "execution_time": execution_time})


if __name__ == '__main__':
    python_data_stream_example()
