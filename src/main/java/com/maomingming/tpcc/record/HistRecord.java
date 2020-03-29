package com.maomingming.tpcc.record;

import com.maomingming.tpcc.RandomGenerator;

import java.util.Date;

public class HistRecord {
    int h_c_id;
    int h_c_d_id;
    int h_c_w_id;
    Date h_date;
    float h_amount;
    String h_data;

    public HistRecord(int c_id, int c_d_id, int c_w_id) {
        this.h_c_id = c_id;
        this.h_c_d_id = c_d_id;
        this.h_c_w_id = c_w_id;
        this.h_date = new Date();
        this.h_amount = 10.00f;
        this.h_data = RandomGenerator.makeAlphaString(12, 24);
    }
}
