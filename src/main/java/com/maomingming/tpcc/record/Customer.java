package com.maomingming.tpcc.record;

import com.google.common.collect.ImmutableMap;
import com.maomingming.tpcc.util.RandomGenerator;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

public class Customer implements Record, Serializable {
    public int c_id;
    public int c_d_id;
    public int c_w_id;
    public String c_last;
    public String c_middle;
    public String c_first;
    public String c_street_1;
    public String c_street_2;
    public String c_city;
    public String c_state;
    public String c_zip;
    public String c_phone;
    public Date c_since;
    public String c_credit;
    public BigDecimal c_credit_lim;
    public BigDecimal c_discount;
    public BigDecimal c_balance;
    public BigDecimal c_ytd_payment;
    public int c_payment_cnt;
    public int c_delivery_cnt;
    public String c_data;

    public Customer() {}

    public Customer(int id, int d_id, int d_w_id) {
        this.c_id = id;
        this.c_d_id = d_id;
        this.c_w_id = d_w_id;
        this.c_last = RandomGenerator.makeLastNameForLoad(id);
        this.c_middle = "OE";
        this.c_first = RandomGenerator.makeAlphaString(8, 16);
        this.c_street_1 = RandomGenerator.makeAlphaString(10, 20);
        this.c_street_2 = RandomGenerator.makeAlphaString(10, 20);
        this.c_city = RandomGenerator.makeAlphaString(10, 20);
        this.c_state = RandomGenerator.makeAlphaString(2, 2);
        this.c_zip = RandomGenerator.makeZip();
        this.c_phone = RandomGenerator.makeNumString(16, 16);
        this.c_since = new Date();
        if (RandomGenerator.makeBool(0.1f))
            this.c_credit = "BC";
        else
            this.c_credit = "GC";
        this.c_credit_lim = new BigDecimal("50000.00");
        this.c_discount = RandomGenerator.makeDecimal(0, 5000, 4);
        this.c_balance = new BigDecimal("-10.00");
        this.c_ytd_payment = new BigDecimal("10.00");
        this.c_payment_cnt = 1;
        this.c_delivery_cnt = 0;
        this.c_data = RandomGenerator.makeAlphaString(300, 500);
    }

    public static String getKey(Map<String, Object> key) {
        if (key.containsKey("c_w_id") && key.containsKey("c_d_id") && key.containsKey("c_id"))
            return getKey((int) key.get("c_w_id"), (int) key.get("c_d_id"), (int) key.get("c_id"));
        return null;
    }

    public static String getKey(int c_w_id, int c_d_id, int c_id) {
        return "C_W_ID=" + c_w_id + "&C_D_ID=" + c_d_id + "&C_ID=" + c_id;
    }

    public String getKey() {
        return getKey(this.c_w_id, this.c_d_id, this.c_id);
    }

    public Map<String, Object> getKeyMap() {
        return ImmutableMap.<String, Object>builder()
                .put("c_w_id", c_w_id)
                .put("c_d_id", c_d_id)
                .put("c_id", c_id)
                .build();
    }
}
