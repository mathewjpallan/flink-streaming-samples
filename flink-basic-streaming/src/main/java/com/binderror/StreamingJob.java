package com.binderror;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.Serializers;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.File;

public class StreamingJob extends BaseStreamingJob {

    public static void main(String[] args) throws Exception {


        JobConfig jobConfig = loadJobConfig(args[0], JobConfig.class);
        StreamExecutionEnvironment env = initStreamingEnv(jobConfig);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(jobConfig.getKafkaSourceBrokers())
                .setTopics(jobConfig.getStream1().getSource())
                .setGroupId(jobConfig.getJobConsumerGroupName())
                .setStartingOffsets(OffsetsInitializer.committedOffsets("earliest".equals(jobConfig.getStartingOffset()) ? OffsetResetStrategy.EARLIEST : OffsetResetStrategy.LATEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
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

        DataStream<String> stream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").setParallelism(jobConfig.getStream1().getSourceParallelism());

        stream
            .map(new UpperCaseMapper()).setParallelism(jobConfig.getStream1().getMapParallelism())
            .sinkTo(sink).setParallelism(jobConfig.getStream1().getSinkParallelism());

        env.execute("flink-basic-streaming");
    }


}
