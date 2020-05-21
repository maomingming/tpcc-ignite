package com.maomingming.tpcc.record;

import com.google.common.collect.ImmutableMap;
import com.maomingming.tpcc.util.RandomGenerator;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Warehouse implements Record, Serializable {
    public int w_id;
    public String w_name;
    public String w_street_1;
    public String w_street_2;
    public String w_city;
    public String w_state;
    public String w_zip;
    public BigDecimal w_tax;
    public BigDecimal w_ytd;

    public Warehouse(){}

    public Warehouse(int id) {
        this.w_id = id;
        this.w_name = RandomGenerator.makeAlphaString(6, 10);
        this.w_street_1 = RandomGenerator.makeAlphaString(10, 20);
        this.w_street_2 = RandomGenerator.makeAlphaString(10, 20);
        this.w_city = RandomGenerator.makeAlphaString(10, 20);
        this.w_state = RandomGenerator.makeAlphaString(2,2);
        this.w_zip = RandomGenerator.makeZip();
        this.w_tax = RandomGenerator.makeDecimal(0, 2000, 4);
        this.w_ytd = new BigDecimal("300000.00");
    }

    public static String getKey(Map<String, Object> key) {
        if (key.containsKey("w_id"))
            return getKey((int) key.get("w_id"));
        return null;
    }

    public static String getKey(int w_id) {
        return "W_ID=" + w_id;
    }

    public String getKey() {
        return getKey(this.w_id);
    }

    public Map<String, Object> getKeyMap() {
        return ImmutableMap.<String, Object>builder()
                .put("w_id", w_id)
                .build();
    }
}
