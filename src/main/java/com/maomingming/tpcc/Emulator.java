package com.maomingming.tpcc;

import com.maomingming.tpcc.RandomGenerator;
import com.maomingming.tpcc.load.Loader;
import com.maomingming.tpcc.load.StreamLoader;
import com.maomingming.tpcc.txn.Executor;
import com.maomingming.tpcc.txn.KeyValueExecutor;

public class Emulator extends Thread{
    int w_id;
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

    public Emulator(String executorType, int w_id) {
        this.w_id = w_id;
        this.executor = getExecutor(executorType, w_id);
    }

    private Executor getExecutor(String executorType, int w_id) {
        switch (executorType) {
            case "KEY_VALUE_EXECUTOR":
                return new KeyValueExecutor(w_id);
            default:
                throw new RuntimeException("Wrong executor Type.");
        }
    }

    public void doNext() {
        doNewOrder();
    }

    public void doNewOrder() {
//        int d_id = RandomGenerator.makeNumber(1, 10);
//        int c_id = RandomGenerator.makeNURand(1023, 1, 3000);
//        int ol_cnt = RandomGenerator.makeNumber(5, 15);
//        int rbk = RandomGenerator.makeNumber(1, 100);
        this.executor.doNewOrder(this.w_id, 1, 1, 1, null);

    }

}
