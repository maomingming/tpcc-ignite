package com.maomingming.tpcc.execute;

import com.maomingming.tpcc.txn.NewOrderTxn;

public interface Executor {
    int doNewOrder(NewOrderTxn newOrderTxn);
    void executeFinish();
}
