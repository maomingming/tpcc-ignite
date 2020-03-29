package com.maomingming.tpcc.record;

import com.maomingming.tpcc.RandomGenerator;

public class DistRecord {
    int d_id;
    int d_w_id;
    String d_name;
    String d_street_1;
    String d_street_2;
    String d_city;
    String d_state;
    String d_zip;
    float d_tax;
    float d_ytd;
    int d_next_o_id;

    public DistRecord(int id, int w_id) {
        this.d_id = id;
        this.d_w_id = w_id;
        this.d_name = RandomGenerator.makeAlphaString(6, 10);
        this.d_street_1 = RandomGenerator.makeAlphaString(10, 20);
        this.d_street_2 = RandomGenerator.makeAlphaString(10, 20);
        this.d_city = RandomGenerator.makeAlphaString(10, 20);
        this.d_state = RandomGenerator.makeAlphaString(2,2);
        this.d_zip = RandomGenerator.makeZip();
        this.d_tax = RandomGenerator.makeFloat(0.0000f, 0.2000f, 0.0001f);
        this.d_ytd = 30000.00f;
        this.d_next_o_id = 3001;
    }
}

