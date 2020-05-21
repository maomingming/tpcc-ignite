package com.maomingming.tpcc.param;

public class Aggregation {
    public String aggregationType;
    public String column;
    public String dataType;

    public Aggregation(String aggregationType) {
        assert aggregationType.equals("COUNT");
        this.aggregationType = aggregationType;
    }

    public Aggregation(String aggregationType, String column, String dataType) {
        assert aggregationType.equals("SUM");
        assert dataType.equals("DECIMAL");
        this.aggregationType = aggregationType;
        this.column = column;
        this.dataType = dataType;
    }
}
