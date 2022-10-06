package com.binderror;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.UUID;

public class SampleDataSource extends RichParallelSourceFunction<Data> {

    private volatile boolean cancelled = false;

    private int counter = 1;

    @Override
    public void run(SourceContext<Data> sourceContext) throws Exception {
        while(!cancelled) {
            sourceContext.collect(new Data(UUID.randomUUID().toString(), "reg" + counter, System.currentTimeMillis(), 1));
            if (++counter == 4) counter = 1;
            Thread.sleep(5000);
        }
    }

    @Override
    public void cancel() {
        this.cancelled = true;
    }
}
