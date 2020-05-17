package com.maomingming.tpcc.record;

import com.maomingming.tpcc.util.RandomGenerator;

import java.math.BigDecimal;

public class District implements Record{
    public int d_id;
    public int d_w_id;
    public String d_name;
    public String d_street_1;
    public String d_street_2;
    public String d_city;
    public String d_state;
    public String d_zip;
    public BigDecimal d_tax;
    public BigDecimal d_ytd;
    public int d_next_o_id;

    public District(int id, int w_id) {
        this.d_id = id;
        this.d_w_id = w_id;
        this.d_name = RandomGenerator.makeAlphaString(6, 10);
        this.d_street_1 = RandomGenerator.makeAlphaString(10, 20);
        this.d_street_2 = RandomGenerator.makeAlphaString(10, 20);
        this.d_city = RandomGenerator.makeAlphaString(10, 20);
        this.d_state = RandomGenerator.makeAlphaString(2,2);
        this.d_zip = RandomGenerator.makeZip();
        this.d_tax = RandomGenerator.makeDecimal(0, 2000, 4);
        this.d_ytd = new BigDecimal("30000.00");
        this.d_next_o_id = 3001;
    }

    public static String getKey(int d_w_id, int d_id) {
        return "D_W_ID=" + d_w_id + "&D_ID=" + d_id;
    }

    public String getKey() {
        return getKey(this.d_w_id, this.d_id);
    }
}

