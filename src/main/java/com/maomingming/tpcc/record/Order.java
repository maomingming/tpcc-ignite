package com.maomingming.tpcc.record;

import com.maomingming.tpcc.util.RandomGenerator;

import java.util.Date;

public class Order implements Record{
    public int o_id;
    public int o_d_id;
    public int o_w_id;
    public int o_c_id;
    public Date o_entry_d;
    public int o_carrier_id;
    public int o_ol_cnt;
    public boolean o_all_local;

    public Order(int id, int c_id, int d_id, int w_id) {
        this.o_id = id;
        this.o_c_id = c_id;
        this.o_d_id = d_id;
        this.o_w_id = w_id;
        this.o_entry_d = new Date();
        if (id < 2101)
            this.o_carrier_id = 0;
        else
            this.o_carrier_id = RandomGenerator.makeNumber(1, 10);
        this.o_ol_cnt = RandomGenerator.makeNumber(5, 15);
        this.o_all_local = true;
    }

    public Order(int id, int c_id, int d_id, int w_id, int ol_cnt, boolean all_local) {
        this.o_id = id;
        this.o_c_id = c_id;
        this.o_d_id = d_id;
        this.o_w_id = w_id;
        this.o_entry_d = new Date();
        this.o_carrier_id = 0;
        this.o_ol_cnt = ol_cnt;
        this.o_all_local = all_local;
    }

    public OrderLine[] makeOrdLineForLoad() {
        OrderLine[] ordLine = new OrderLine[this.o_ol_cnt];
        for (int i = 0; i < this.o_ol_cnt; i++) {
            ordLine[i] = new OrderLine(this.o_id, this.o_d_id, this.o_w_id, i + 1, this.o_entry_d);
        }
        return ordLine;
    }

    public static String getKey(int o_w_id, int o_d_id, int o_id) {
        return "O_W_ID=" + o_w_id + "&O_D_ID=" + o_d_id + "&O_ID" + o_id;
    }

    public String getKey() {
        return getKey(this.o_w_id, this.o_d_id, this.o_id);
    }
}
