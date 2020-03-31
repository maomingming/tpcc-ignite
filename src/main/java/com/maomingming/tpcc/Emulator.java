package com.maomingming.tpcc;

import com.maomingming.tpcc.execute.Executor;
import com.maomingming.tpcc.execute.KeyValueExecutor;
import com.maomingming.tpcc.txn.NewOrder;

public class Emulator extends Thread{
    int w_id;
    int w_cnt;
    Executor executor;

    public void run() {
        for (int i = 0; i < 1000; i ++) {
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
        NewOrder newOrder = new NewOrder(w_id, w_cnt);
        int ret = this.executor.doNewOrder(newOrder);
        if (ret != 0)
            newOrder.printAfterRollback(System.out);
        else
            newOrder.printResult(System.out);
    }

}
