package com.maomingming.tpcc.param;

import java.util.Map;

public class Query {
    public Map<String, Object> equal;
    public Map<String, Object[]> in;
    public Map<String, Object> lessThan;
    public Map<String, Object> greaterThan;

    public Query(Map<String, Object> equal) {
        this.equal = equal;
    }
}
