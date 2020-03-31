package com.maomingming.tpcc;

import com.maomingming.tpcc.record.OrdLineRecord;
import com.maomingming.tpcc.execute.Executor;
import com.maomingming.tpcc.execute.KeyValueExecutor;

public class Emulator extends Thread{
    int w_id;
    int w_cnt;
    Executor executor;

    public void run() {
        for (int i = 0; i < 5; i ++) {
            doNext();
            try {
                sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.executor.executeFinish();
    }

    public Emulator(String executorType, int w_id, int w_cnt) {
        this.w_id = w_id;
        this.w_cnt = w_cnt;
        this.executor = getExecutor(executorType, w_id);
    }

    private Executor getExecutor(String executorType, int w_id) {
        switch (executorType) {
            case "KEY_VALUE_EXECUTOR":
                return new KeyValueExecutor(w_id);
            default:
                throw new IllegalStateException("Unexpected value: " + executorType);
        }
    }

    public void doNext() {
        doNewOrder();
    }

    public void doNewOrder() {
        int d_id = RandomGenerator.makeNumber(1, 10);
        int c_id = RandomGenerator.makeNURand(1023, 1, 3000);
        int ol_cnt = RandomGenerator.makeNumber(5, 15);
        int rbk = RandomGenerator.makeNumber(1, 100);

        OrdLineRecord[] ordLineRecords = new OrdLineRecord[ol_cnt];
        for (int i = 1; i <= ol_cnt; i ++) {
            ordLineRecords[i-1] = new OrdLineRecord(d_id, this.w_id, i, i==ol_cnt && rbk==1, this.w_cnt);
        }

        this.executor.doNewOrder(this.w_id, d_id, c_id, ol_cnt, ordLineRecords);

    }

}
