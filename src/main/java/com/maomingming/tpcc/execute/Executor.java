package com.maomingming.tpcc.execute;

import com.maomingming.tpcc.record.OrdLineRecord;

public interface Executor {
    void doNewOrder(int w_id, int d_id, int c_id, int ol_cnt, OrdLineRecord[] ordLineRecords);
    void executeFinish();
}
