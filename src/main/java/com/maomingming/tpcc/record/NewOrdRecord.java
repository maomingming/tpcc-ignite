package com.maomingming.tpcc.record;

public class NewOrdRecord implements Record{
    public int no_o_id;
    public int no_d_id;
    public int no_w_id;

    public NewOrdRecord(int o_id, int d_id, int w_id) {
        this.no_o_id = o_id;
        this.no_d_id = d_id;
        this.no_w_id = w_id;
    }

    public static String getKey(int no_w_id, int no_d_id, int no_o_id) {
        return "NO_W_ID=" + no_w_id + "&NO_D_ID=" + no_d_id + "&NO_O_ID" + no_o_id;
    }

    public String getKey() {
        return getKey(this.no_w_id, this.no_d_id, this.no_o_id);
    }
}
