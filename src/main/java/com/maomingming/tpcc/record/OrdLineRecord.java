package com.maomingming.tpcc.record;

import com.maomingming.tpcc.RandomGenerator;

import java.util.Date;

public class OrdLineRecord implements Record {
    int ol_o_id;
    int ol_d_id;
    int ol_w_id;
    int ol_number;
    int ol_i_id;
    int ol_supply_w_id;
    Date ol_delivery_d;
    int ol_quantity;
    float ol_amount;
    String ol_dist_info;

    public OrdLineRecord(int o_id, int d_id, int w_id, int number, Date entryD) {
        this.ol_o_id = o_id;
        this.ol_d_id = d_id;
        this.ol_w_id = w_id;
        this.ol_number = number;
        this.ol_i_id = RandomGenerator.makeNumber(1, 100000);
        this.ol_supply_w_id = w_id;
        if (o_id < 2101)
            this.ol_delivery_d = entryD;
        this.ol_quantity = 5;
        if (o_id < 2101)
            this.ol_amount = 0.00f;
        else
            this.ol_amount = RandomGenerator.makeFloat(0.01f, 9999.99f, 0.01f);
        this.ol_dist_info = RandomGenerator.makeAlphaString(24, 24);
    }

    public static String getKey(int ol_w_id, int ol_d_id, int ol_o_id, int ol_number) {
        return "OL_W_ID=" + ol_w_id + "&OL_D_ID=" + ol_d_id + "&OL_O_ID" + ol_o_id + "&OL_NUMBER" + ol_number;
    }

    public String getKey() {
        return getKey(this.ol_w_id, this.ol_d_id, this.ol_o_id, this.ol_number);
    }
}
