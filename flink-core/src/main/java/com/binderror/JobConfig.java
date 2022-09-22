package com.binderror;

import lombok.Data;

@Data
public class JobConfig {

    private String jobName;
    private String jobConsumerGroupName;
    private String kafkaSourceBrokers;
    private String kafkaSinkBrokers;
    private String startingOffset;
    private int restartCount;
    private long restartDelay;
    private StreamConfig stream1;
    private CheckpointConfig checkpointConfig;
}
