package com.binderror;

import lombok.Data;

@Data
public class CheckpointConfig {
    private boolean enabled;
    private long checkpointInterval;
    private long pauseBtwCheckpoints;
    private long checkpointTimeout;
    private int maxConcurrentCheckpoints;
    private boolean preferCheckpointForRecovery;
    private String checkpointMode;
    private String checkpointDataUri;
}
