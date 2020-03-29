package com.maomingming.tpcc.record;

import com.maomingming.tpcc.RandomGenerator;

import java.util.Date;

public class CustRecord {
    int c_id;
    int c_d_id;
    int c_w_id;
    String c_last;
    String c_middle;
    String c_first;
    String c_street_1;
    String c_street_2;
    String c_city;
    String c_state;
    String c_zip;
    String c_phone;
    Date c_since;
    String c_credit;
    float c_credit_lim;
    float c_discount;
    float c_balance;
    float c_ytd_payment;
    int c_payment_cnt;
    int c_delivery_cnt;
    String c_data;

    public CustRecord(int id, int d_id, int d_w_id) {
        this.c_id = id;
        this.c_d_id = d_id;
        this.c_w_id = d_w_id;
        this.c_last = RandomGenerator.makeLastName(id);
        this.c_middle = "OE";
        this.c_first = RandomGenerator.makeAlphaString(8, 16);
        this.c_street_1 = RandomGenerator.makeAlphaString(10, 20);
        this.c_street_2 = RandomGenerator.makeAlphaString(10, 20);
        this.c_city = RandomGenerator.makeAlphaString(10, 20);
        this.c_state = RandomGenerator.makeAlphaString(2,2);
        this.c_zip = RandomGenerator.makeZip();
        this.c_phone = RandomGenerator.makeNumString(16, 16);
        this.c_since = new Date();
        if (RandomGenerator.makeBool(0.1f))
            this.c_credit = "BC";
        else
            this.c_credit = "GC";
        this.c_credit_lim = 50000.00f;
        this.c_discount = RandomGenerator.makeFloat(0.0f, 0.5f, 0.0001f);
        this.c_balance = -10.00f;
        this.c_ytd_payment = 10.00f;
        this.c_payment_cnt = 1;
        this.c_delivery_cnt = 0;
        this.c_data = RandomGenerator.makeAlphaString(300, 500);
    }
}
