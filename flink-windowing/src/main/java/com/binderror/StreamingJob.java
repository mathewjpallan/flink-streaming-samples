package com.binderror;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;

public class StreamingJob extends BaseStreamingJob {

    public static void main(String[] args) throws Exception {


        JobConfig jobConfig = loadJobConfig(args[0], JobConfig.class);

        StreamExecutionEnvironment env = initStreamingEnv(jobConfig);

        KafkaSource<Data> source = KafkaSource.<Data>builder()
                .setBootstrapServers(jobConfig.getKafkaSourceBrokers())
                .setTopics(jobConfig.getStream1().getSource())
                .setGroupId(jobConfig.getJobConsumerGroupName())
                .setStartingOffsets(OffsetsInitializer.committedOffsets("earliest".equals(jobConfig.getStartingOffset()) ? OffsetResetStrategy.EARLIEST : OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new JsonDataDeserializer<>(TypeInformation.of(Data.class)))
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(jobConfig.getKafkaSinkBrokers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(jobConfig.getStream1().getSink())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        WatermarkStrategy<Data> strategy = WatermarkStrategy
                .<Data>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());

        WatermarkStrategy<Data> strategy2 = new WatermarkStrategy<Data>() {
            @Override
            public WatermarkGenerator<Data> createWatermarkGenerator(
                    WatermarkGeneratorSupplier.Context context) {
                return new BoundedOutOfOrdernessWatermarks<>(
                        Duration.ofMillis(0)
                ) {
                    @Override
                    public void onEvent(
                            Data event,
                            long eventTimestamp,
                            WatermarkOutput output) {
                        super.onEvent(event, eventTimestamp, output);
                        super.onPeriodicEmit(output);
                    }
                };
            }
        };
        strategy2.withTimestampAssigner((event, timestamp) -> event.getTimestamp());
        //DataStream<Data> stream = env.addSource(new SampleDataSource()).setParallelism(jobConfig.getStream1().getSourceParallelism()).assignTimestampsAndWatermarks(strategy);
        DataStream<Data> stream = env.fromSource(source, strategy2, "kafkasrc");
        stream.keyBy(event -> event.getRegion())
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .process(new SummaryWindowFunction())
                .map(new ObjectToJsonMap<SummaryData>())
                .sinkTo(sink).setParallelism(jobConfig.getStream1().getSinkParallelism());

        env.execute("flink-basic-streaming");
    }


}
