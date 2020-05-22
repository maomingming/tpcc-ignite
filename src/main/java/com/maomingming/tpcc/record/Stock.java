package com.maomingming.tpcc.record;

import com.google.common.collect.ImmutableMap;
import com.maomingming.tpcc.util.RandomGenerator;

import java.io.Serializable;
import java.util.Map;

public class Stock implements Record, Serializable {
    public int s_i_id;
    public int s_w_id;
    public int s_quantity;
    public String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
    public String s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10;
    public int s_ytd;
    public int s_order_cnt;
    public int s_remote_cnt;
    public String s_data;

    public Stock() {}

    public Stock(int i_id, int w_id) {
        this.s_i_id = i_id;
        this.s_w_id = w_id;
        this.s_quantity = RandomGenerator.makeNumber(10, 100);
        this.s_dist_01 = RandomGenerator.makeAlphaString(24, 24);
        this.s_dist_02 = RandomGenerator.makeAlphaString(24, 24);
        this.s_dist_03 = RandomGenerator.makeAlphaString(24, 24);
        this.s_dist_04 = RandomGenerator.makeAlphaString(24, 24);
        this.s_dist_05 = RandomGenerator.makeAlphaString(24, 24);
        this.s_dist_06 = RandomGenerator.makeAlphaString(24, 24);
        this.s_dist_07 = RandomGenerator.makeAlphaString(24, 24);
        this.s_dist_08 = RandomGenerator.makeAlphaString(24, 24);
        this.s_dist_09 = RandomGenerator.makeAlphaString(24, 24);
        this.s_dist_10 = RandomGenerator.makeAlphaString(24, 24);
        this.s_ytd = 0;
        this.s_order_cnt = 0;
        this.s_remote_cnt = 0;
        this.s_data = RandomGenerator.makeAlphaString(26, 50);
        if (RandomGenerator.makeBool(0.1f))
            this.s_data = RandomGenerator.fillOriginal(this.s_data);
    }

    public static String getKey(Map<String, Object> key) {
        if (key.containsKey("s_w_id") && key.containsKey("s_i_id"))
            return getKey((int) key.get("s_w_id"), (int) key.get("s_i_id"));
        return null;
    }

    public static String getKey(int s_w_id, int s_i_id) {
        return "S_W_ID=" + s_w_id + "&S_I_ID=" + s_i_id;
    }

    public String getKey() {
        return getKey(this.s_w_id, this.s_i_id);
    }

    public Map<String, Object> getKeyMap() {
        return ImmutableMap.<String, Object>builder()
                .put("s_w_id", s_w_id)
                .put("s_i_id", s_i_id)
                .build();
    }
}
