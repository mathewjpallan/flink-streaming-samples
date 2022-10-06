package com.binderror;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class StreamingJob extends BaseStreamingJob {

    public static void main(String[] args) throws Exception {


        JobConfig jobConfig = loadJobConfig(args[0], JobConfig.class);
        StreamExecutionEnvironment env = initStreamingEnv(jobConfig);

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(jobConfig.getKafkaSinkBrokers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(jobConfig.getStream1().getSink())
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStream<Data> stream = env.addSource(new SampleDataSource()).setParallelism(jobConfig.getStream1().getSourceParallelism());
        WatermarkStrategy<Data> strategy = WatermarkStrategy
                .<Data>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> event.getTimestamp());
        stream.assignTimestampsAndWatermarks(strategy).keyBy(event -> event.getRegion())
                .window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .process(new SummaryWindowFunction())
                .map(new ObjectToJsonMap<SummaryData>())
                .sinkTo(sink).setParallelism(jobConfig.getStream1().getSinkParallelism());

        env.execute("flink-basic-streaming");
    }


}
