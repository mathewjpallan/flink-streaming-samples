package com.binderror;

import lombok.AllArgsConstructor;

@lombok.Data
@AllArgsConstructor
public class Data {
    private String id;
    private String region;
    private long timestamp;
    private float saleValue;
}
