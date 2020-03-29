package com.maomingming.tpcc.record;

import com.maomingming.tpcc.RandomGenerator;

import java.util.Date;

public class OrdRecord {
    int o_id;
    int o_c_id;
    int o_d_id;
    int o_w_id;
    Date o_entry_d;
    int o_carrier;
    int o_ol_cnt;
    int o_all_local;

    public OrdRecord(int id, int c_id, int d_id, int w_id) {
        this.o_id = id;
        this.o_c_id = c_id;
        this.o_d_id = d_id;
        this.o_w_id = w_id;
        this.o_entry_d = new Date();
        if (id < 2101)
            this.o_carrier = 0;
        else
            this.o_carrier = RandomGenerator.makeNumber(1, 10);
        this.o_ol_cnt = RandomGenerator.makeNumber(5, 15);
        this.o_all_local = 1;
    }
}
