package com.binderror;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class SummaryWindowFunction extends ProcessWindowFunction<Data, SummaryData, String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<Data, SummaryData, String, TimeWindow>.Context context, Iterable<Data> iterable, Collector<SummaryData> collector) throws Exception {
        Float value = 0F;
        for (Iterator<Data> it = iterable.iterator(); it.hasNext(); ) {
            Data data = it.next();
            value += data.getSaleValue();
        }

        collector.collect(new SummaryData(s, value, context.currentProcessingTime() + "=:=" + context.currentWatermark() + "=:=" + context.window()));

    }
}
