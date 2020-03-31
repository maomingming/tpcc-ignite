package com.maomingming.tpcc.execute;

import com.maomingming.tpcc.txn.NewOrder;

public interface Executor {
    void doNewOrder(NewOrder newOrder);
    void executeFinish();
}
