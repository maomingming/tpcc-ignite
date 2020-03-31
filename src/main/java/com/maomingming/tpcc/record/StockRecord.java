package com.maomingming.tpcc.record;

import com.maomingming.tpcc.RandomGenerator;

public class StockRecord implements Record{
    public int s_i_id;
    public int s_w_id;
    public int s_quantity;
    public String s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05;
    public String s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10;
    public int s_ytd;
    public int s_order_cnt;
    public int s_remote_cnt;
    public String s_data;

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

    public String getDistInfo(int d_id) {
        switch (d_id) {
            case 1:
                return s_dist_01;
            case 2:
                return s_dist_02;
            case 3:
                return s_dist_03;
            case 4:
                return s_dist_04;
            case 5:
                return s_dist_05;
            case 6:
                return s_dist_06;
            case 7:
                return s_dist_07;
            case 8:
                return s_dist_08;
            case 9:
                return s_dist_09;
            case 10:
                return s_dist_10;
            default:
                throw new IllegalStateException("Unexpected value: " + d_id);
        }
    }
}
