package com.maomingming.tpcc.record;

import com.maomingming.tpcc.RandomGenerator;

import java.util.Date;

public class OrdRecord implements Record{
    public int o_id;
    public int o_c_id;
    public int o_d_id;
    public int o_w_id;
    public Date o_entry_d;
    public int o_carrier;
    public int o_ol_cnt;
    public int o_all_local;

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

    public OrdLineRecord[] makeOrdLineForLoad() {
        OrdLineRecord[] ordLine = new OrdLineRecord[this.o_ol_cnt];
        for (int i = 0; i < this.o_ol_cnt; i++) {
            ordLine[i] = new OrdLineRecord(this.o_id, this.o_d_id, this.o_w_id, i + 1, this.o_entry_d);
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
