package com.maomingming.tpcc.param;

import java.util.Map;
import java.util.Set;

public class Query {
    public Map<String, Object> equal;
    public Map<String, Set<Integer>> in;
    public Map<String, Comparable<?>> lessThan;

    public Query(Map<String, Object> equal) {
        this.equal = equal;
    }

    public Query(Map<String, Object> equal, Map<String, Set<Integer>> in) {
        this.equal = equal;
        this.in = in;
    }

    public Query(Map<String, Object> equal, Map<String, Set<Integer>> in, Map<String, Comparable<?>> lessThan) {
        this.equal = equal;
        this.in = in;
        this.lessThan = lessThan;
    }

}
