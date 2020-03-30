package com.maomingming.tpcc;

import com.maomingming.tpcc.RandomGenerator;

public class Emulator {
    int w_id;

    public Emulator(int w_id) {
        this.w_id = w_id;
    }

    public void doNewOrder() {
        int d_id = RandomGenerator.makeNumber(1, 10);
        int c_id = RandomGenerator.makeNURand(1023, 1, 3000);
        int ol_cnt = RandomGenerator.makeNumber(5, 15);
        int rbk = RandomGenerator.makeNumber(1, 100);

    }

}
