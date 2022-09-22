package com.binderror;

import lombok.Data;

@Data
public class StreamConfig {
    private String source;
    private String sink;
    private int sourceParallelism;
    private int mapParallelism;
    private int sinkParallelism;

}
