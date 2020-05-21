package com.maomingming.tpcc.record;

import com.google.common.collect.ImmutableMap;
import com.maomingming.tpcc.util.RandomGenerator;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class District implements Record, Serializable {
    public int d_id;
    public int d_w_id;
    public String d_name;
    public String d_street_1;
    public String d_street_2;
    public String d_city;
    public String d_state;
    public String d_zip;
    public BigDecimal d_tax;
    public BigDecimal d_ytd;
    public int d_next_o_id;

    public District() {}

    public District(int id, int w_id) {
        this.d_id = id;
        this.d_w_id = w_id;
        this.d_name = RandomGenerator.makeAlphaString(6, 10);
        this.d_street_1 = RandomGenerator.makeAlphaString(10, 20);
        this.d_street_2 = RandomGenerator.makeAlphaString(10, 20);
        this.d_city = RandomGenerator.makeAlphaString(10, 20);
        this.d_state = RandomGenerator.makeAlphaString(2,2);
        this.d_zip = RandomGenerator.makeZip();
        this.d_tax = RandomGenerator.makeDecimal(0, 2000, 4);
        this.d_ytd = new BigDecimal("30000.00");
        this.d_next_o_id = 3001;
    }

    public static String getKey(Map<String, Object> key) {
        if (key.containsKey("d_w_id") && key.containsKey("d_id"))
            return getKey((int) key.get("d_w_id"), (int) key.get("d_id"));
        return null;
    }

    public static String getKey(int d_w_id, int d_id) {
        return "D_W_ID=" + d_w_id + "&D_ID=" + d_id;
    }

    public String getKey() {
        return getKey(this.d_w_id, this.d_id);
    }

    public Map<String, Object> getKeyMap() {
        return ImmutableMap.<String, Object>builder()
                .put("d_w_id", d_w_id)
                .put("d_id", d_id)
                .build();
    }
}

