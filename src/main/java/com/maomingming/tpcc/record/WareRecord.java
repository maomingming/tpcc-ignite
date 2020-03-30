package com.maomingming.tpcc.record;

import com.maomingming.tpcc.RandomGenerator;

public class WareRecord implements Record {
    public int w_id;
    public String w_name;
    public String w_street_1;
    public String w_street_2;
    public String w_city;
    public String w_state;
    public String w_zip;
    public float w_tax;
    public float w_ytd;

    public WareRecord(int id) {
        this.w_id = id;
        this.w_name = RandomGenerator.makeAlphaString(6, 10);
        this.w_street_1 = RandomGenerator.makeAlphaString(10, 20);
        this.w_street_2 = RandomGenerator.makeAlphaString(10, 20);
        this.w_city = RandomGenerator.makeAlphaString(10, 20);
        this.w_state = RandomGenerator.makeAlphaString(2,2);
        this.w_zip = RandomGenerator.makeZip();
        this.w_tax = RandomGenerator.makeFloat(0.0000f, 0.2000f, 0.0001f);
        this.w_ytd = 300000.00f;
    }

    public static String getKey(int w_id) {
        return "W_ID=" + w_id;
    }

    public String getKey() {
        return getKey(this.w_id);
    }
}
