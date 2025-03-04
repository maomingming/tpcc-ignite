package com.maomingming.tpcc.record;

import com.google.common.collect.ImmutableMap;
import com.maomingming.tpcc.util.RandomGenerator;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

public class OrderLine implements Record, Serializable {
    public int ol_o_id;
    public int ol_d_id;
    public int ol_w_id;
    public int ol_number;
    public int ol_i_id;
    public int ol_supply_w_id;
    public Date ol_delivery_d;
    public int ol_quantity;
    public BigDecimal ol_amount;
    public String ol_dist_info;

    public OrderLine() {}

    public OrderLine(int o_id, int d_id, int w_id, int number, Date entryD) {
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
            this.ol_amount = new BigDecimal("0.00");
        else
            this.ol_amount = RandomGenerator.makeDecimal(1, 999999, 2);
        this.ol_dist_info = RandomGenerator.makeAlphaString(24, 24);
    }

    public OrderLine(int o_id, int d_id, int w_id, int number, int i_id, int supply_w_id, int quantity, BigDecimal amount, String dist_info) {
        this.ol_o_id = o_id;
        this.ol_d_id = d_id;
        this.ol_w_id = w_id;
        this.ol_number = number;
        this.ol_i_id = i_id;
        this.ol_supply_w_id = supply_w_id;
        this.ol_quantity = quantity;
        this.ol_amount = amount;
        this.ol_dist_info = dist_info;
    }

    public static String getKey(Map<String, Object> key) {
        if (key.containsKey("ol_w_id") && key.containsKey("ol_d_id") && key.containsKey("ol_o_id") && key.containsKey("ol_number"))
            return getKey((int) key.get("ol_w_id"), (int) key.get("ol_d_id"), (int) key.get("ol_o_id"), (int) key.get("ol_number"));
        return null;
    }


    public static String getKey(int ol_w_id, int ol_d_id, int ol_o_id, int ol_number) {
        return "OL_W_ID=" + ol_w_id + "&OL_D_ID=" + ol_d_id + "&OL_O_ID" + ol_o_id + "&OL_NUMBER" + ol_number;
    }

    public String getKey() {
        return getKey(this.ol_w_id, this.ol_d_id, this.ol_o_id, this.ol_number);
    }

    public Map<String, Object> getKeyMap() {
        return ImmutableMap.<String, Object>builder()
                .put("ol_w_id", ol_w_id)
                .put("ol_d_id", ol_d_id)
                .put("ol_o_id", ol_o_id)
                .put("ol_number", ol_number)
                .build();
    }
}
