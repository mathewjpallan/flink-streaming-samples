package com.binderror;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SummaryData {

    private String region;
    private Float value;
    private String timeWindow;

}
