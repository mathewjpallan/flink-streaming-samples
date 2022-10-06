package com.binderror;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

public class ObjectToJsonMap<T> implements MapFunction<T, String> {

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public String map(T object) throws Exception {
        return mapper.writeValueAsString(object);
    }
}
