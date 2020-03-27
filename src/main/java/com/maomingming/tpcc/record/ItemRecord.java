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

    public ItemRecord(int id) {
        this.i_id = id;
        this.i_im_id = RandomGenerator.makeNumber(0, 10000);
        this.i_name = RandomGenerator.makeAlphaString(0, 10);
        this.i_price = RandomGenerator.makeNumber(0, 100);
        this.i_data = RandomGenerator.makeAlphaString(0, 10);
    }

}
