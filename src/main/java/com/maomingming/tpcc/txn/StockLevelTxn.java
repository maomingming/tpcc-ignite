package com.maomingming.tpcc.txn;

import com.maomingming.tpcc.util.RandomGenerator;

public class StockLevelTxn {
    public int w_id;
    public int d_id;
    public int threshold;
    public int low_stock;

    public StockLevelTxn(int w_id, int t_id) {
        this.w_id = w_id;
        d_id = t_id;
        threshold = RandomGenerator.makeNumber(10, 20);
    }
}
