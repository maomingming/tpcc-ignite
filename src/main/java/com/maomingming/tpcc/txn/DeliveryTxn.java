package com.maomingming.tpcc.txn;

import com.maomingming.tpcc.RandomGenerator;

public class DeliveryTxn {
    public int w_id;
    public int o_carrier_id;

    public DeliveryTxn(int w_id) {
        this.w_id = w_id;
        o_carrier_id = RandomGenerator.makeNumber(1, 10);
    }
}
