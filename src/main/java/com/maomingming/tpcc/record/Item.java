package com.maomingming.tpcc.record;

import com.maomingming.tpcc.util.RandomGenerator;

public class Item implements Record {

    public int i_id;
    public int i_im_id;
    public String i_name;
    public float i_price;
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
        this.i_price = RandomGenerator.makeFloat(1.00f, 100.00f, 0.01f);
        this.i_data = RandomGenerator.makeAlphaString(26, 50);
        if (RandomGenerator.makeBool(0.1f))
            this.i_data = RandomGenerator.fillOriginal(this.i_data);
    }

}
