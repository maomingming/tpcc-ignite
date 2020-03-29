package com.maomingming.tpcc.record;

import com.maomingming.tpcc.RandomGenerator;

public class WareRecord {
    int w_id;
    String w_name;
    String w_street_1;
    String w_street_2;
    String w_city;
    String w_state;
    String w_zip;
    float w_tax;
    float w_ytd;

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
}
