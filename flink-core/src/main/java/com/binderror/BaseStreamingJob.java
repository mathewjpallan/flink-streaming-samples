package com.binderror;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.util.concurrent.TimeUnit;

public class BaseStreamingJob {

    protected static StreamExecutionEnvironment initStreamingEnv(JobConfig jobConfig) {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        checkpointConfiguration(env, jobConfig);
        restartConfiguration(env, jobConfig);
        return env;
    }

    protected static void restartConfiguration(StreamExecutionEnvironment env, JobConfig jobConfig) {
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            jobConfig.getRestartCount(),
            Time.of(jobConfig.getRestartDelay(), TimeUnit.SECONDS)
        ));
    }

    protected static <T> T loadJobConfig(String pathToYaml, Class<T> type) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(pathToYaml), type);

    }
    protected static void checkpointConfiguration(StreamExecutionEnvironment env, JobConfig jobConfig) {
        CheckpointConfig checkpointConfig = jobConfig.getCheckpointConfig();
        if(checkpointConfig.isEnabled()) {
            env.enableCheckpointing(checkpointConfig.getCheckpointInterval());
            env.getCheckpointConfig().setCheckpointTimeout(checkpointConfig.getCheckpointTimeout());
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(checkpointConfig.getPauseBtwCheckpoints());
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(checkpointConfig.getMaxConcurrentCheckpoints());
            env.getCheckpointConfig().setCheckpointingMode(
                    "exactly_once".equals(checkpointConfig.getCheckpointMode()) ? CheckpointingMode.EXACTLY_ONCE: CheckpointingMode.AT_LEAST_ONCE);
            env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                    org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
            env.setStateBackend(new EmbeddedRocksDBStateBackend());
            env.getCheckpointConfig().setCheckpointStorage(checkpointConfig.getCheckpointDataUri());
        }
    }

}
