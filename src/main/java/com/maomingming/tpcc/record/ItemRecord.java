package com.maomingming.tpcc.record;

import com.maomingming.tpcc.RandomGenerator;

public class ItemRecord implements Record {

    int i_id;
    int i_im_id;
    String i_name;
    float i_price;
    String i_data;

    public String getKey() {
        return Integer.toString(i_id);
    }

    public ItemRecord(int id, boolean isOriginal) {
        this.i_id = id;
        this.i_im_id = RandomGenerator.makeNumber(1, 10000);
        this.i_name = RandomGenerator.makeAlphaString(14, 24);
        this.i_price = RandomGenerator.makeFloat(1.00f, 100.00f, 0.01f);
        this.i_data = RandomGenerator.makeAlphaString(26, 50);
        if (RandomGenerator.makeBool(0.1f))
            this.i_data = RandomGenerator.fillOriginal(this.i_data);
    }

}
