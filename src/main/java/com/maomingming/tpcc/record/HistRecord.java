package com.maomingming.tpcc.record;

import com.maomingming.tpcc.RandomGenerator;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class HistRecord implements Record{
    private static AtomicInteger h_cnt = new AtomicInteger(0);
    int h_id;
    public int h_c_id;
    public int h_c_d_id;
    public int h_d_id;
    public int h_c_w_id;
    public int h_w_id;
    public Date h_date;
    public float h_amount;
    public String h_data;

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

    public HistRecord(int c_id, int c_d_id, int c_w_id, int d_id, int w_id, Date date, float amount, String data) {
        this.h_id = h_cnt.incrementAndGet();
        this.h_c_id = c_id;
        this.h_c_d_id = c_d_id;
        this.h_d_id = d_id;
        this.h_c_w_id = c_w_id;
        this.h_w_id = w_id;
        this.h_date = date;
        this.h_amount = amount;
        this.h_data = data;
    }

    public String getKey() {
        return "H_ID=" + h_id;
    }
}
