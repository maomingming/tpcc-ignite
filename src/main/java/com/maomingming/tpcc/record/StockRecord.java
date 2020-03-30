package com.maomingming.tpcc.record;

import com.maomingming.tpcc.RandomGenerator;

public class StockRecord implements Record{
    int s_i_id;
    int s_w_id;
    int s_quantity;
    String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
    String s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10;
    int s_ytd;
    int s_order_cnt;
    int s_remote_cnt;
    String s_data;

    public StockRecord(int i_id, int w_id) {
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

    public static String getKey(int s_w_id, int s_i_id) {
        return "S_W_ID=" + s_w_id + "&S_I_ID=" + s_i_id;
    }

    public String getKey() {
        return getKey(this.s_w_id, this.s_i_id);
    }
}
