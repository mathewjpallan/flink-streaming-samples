package com.binderror;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@lombok.Data
@AllArgsConstructor
@NoArgsConstructor
public class Data {
    private String id;
    private String region;
    private long timestamp;
    private float saleValue;
}
