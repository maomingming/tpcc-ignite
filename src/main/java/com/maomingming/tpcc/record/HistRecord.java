package com.maomingming.tpcc.record;

import com.maomingming.tpcc.RandomGenerator;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class HistRecord implements Record{
    private static AtomicInteger h_cnt = new AtomicInteger(0);
    int h_id;
    int h_c_id;
    int h_c_d_id;
    int h_d_id;
    int h_c_w_id;
    int h_w_id;
    Date h_date;
    float h_amount;
    String h_data;

    public HistRecord(int c_id, int d_id, int w_id) {
        this.h_id = h_cnt.incrementAndGet();
        this.h_c_id = c_id;
        this.h_c_d_id = d_id;
        this.h_d_id = d_id;
        this.h_c_w_id = w_id;
        this.h_w_id = w_id;
        this.h_date = new Date();
        this.h_amount = 10.00f;
        this.h_data = RandomGenerator.makeAlphaString(12, 24);
    }

    public String getKey() {
        return "H_ID=" + h_id;
    }
}
