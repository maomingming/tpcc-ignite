package com.maomingming.tpcc.record;

import com.google.common.collect.ImmutableMap;
import com.maomingming.tpcc.util.RandomGenerator;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class History implements Record, Serializable {
    private static AtomicInteger h_cnt = new AtomicInteger(0);
    public int h_id;
    public int h_c_id;
    public int h_c_d_id;
    public int h_d_id;
    public int h_c_w_id;
    public int h_w_id;
    public Date h_date;
    public BigDecimal h_amount;
    public String h_data;

    public History() {}

    public History(int c_id, int d_id, int w_id) {
        this.h_id = h_cnt.incrementAndGet();
        this.h_c_id = c_id;
        this.h_c_d_id = d_id;
        this.h_d_id = d_id;
        this.h_c_w_id = w_id;
        this.h_w_id = w_id;
        this.h_date = new Date();
        this.h_amount = new BigDecimal("10.00");
        this.h_data = RandomGenerator.makeAlphaString(12, 24);
    }

    public History(int c_id, int c_d_id, int c_w_id, int d_id, int w_id, Date date, BigDecimal amount, String data) {
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

    public Map<String, Object> getKeyMap() {
        return ImmutableMap.<String, Object>builder()
                .put("h_id", h_id)
                .build();
    }
}
