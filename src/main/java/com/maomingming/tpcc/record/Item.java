package com.maomingming.tpcc.record;

import com.maomingming.tpcc.util.RandomGenerator;

import java.math.BigDecimal;

public class Item implements Record {

    public int i_id;
    public int i_im_id;
    public String i_name;
    public BigDecimal i_price;
    public String i_data;

    public static String getKey(int i_id) {
        return "I_ID=" + i_id;
    }
    public String getKey() {
        return getKey(i_id);
    }

    public Item(int id) {
        this.i_id = id;
        this.i_im_id = RandomGenerator.makeNumber(1, 10000);
        this.i_name = RandomGenerator.makeAlphaString(14, 24);
        this.i_price = RandomGenerator.makeDecimal(100, 10000, 2);
        this.i_data = RandomGenerator.makeAlphaString(26, 50);
        if (RandomGenerator.makeBool(0.1f))
            this.i_data = RandomGenerator.fillOriginal(this.i_data);
    }

}
