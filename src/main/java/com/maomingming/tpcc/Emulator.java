package com.maomingming.tpcc;

import com.maomingming.tpcc.execute.Executor;
import com.maomingming.tpcc.execute.KeyValueExecutor;
import com.maomingming.tpcc.execute.SQLExecutor;
import com.maomingming.tpcc.txn.NewOrderTxn;
import com.maomingming.tpcc.txn.PaymentTxn;
import com.maomingming.tpcc.txn.StockLevelTxn;
import com.maomingming.tpcc.util.RandomGenerator;

import java.io.FileNotFoundException;
import java.io.PrintStream;

public class Emulator extends Thread{
    int w_id;
    int t_id;
    int w_cnt;
    PrintStream printStream;
    Worker worker;

    public void run() {
        for (int i = 0; i < 1000000; i ++) {
            doNext();
            Counter.cnt.incrementAndGet();
            int waitTime = 600;
            try {
                sleep(waitTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        worker.finish();
    }

    public Emulator(String executorType, int w_id, int t_id, int w_cnt) throws Exception {
        this.w_id = w_id;
        this.t_id = t_id;
        this.w_cnt = w_cnt;
        try {
            this.printStream = new PrintStream("result/" + t_id +".log");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        worker = new Worker(executorType, w_id);
    }

    private Executor getExecutor(String executorType, int w_id) throws Exception {
        switch (executorType) {
            case "KEY_VALUE_EXECUTOR":
                return new KeyValueExecutor();
            case "SQL_EXECUTOR":
                return new SQLExecutor();
            default:
                throw new IllegalStateException("Unexpected value: " + executorType);
        }
    }

    public void doNext() {
        doNewOrder();
    }

    public void doNewOrder() {
        NewOrderTxn newOrderTxn = new NewOrderTxn(w_id, w_cnt);
        long begin = System.currentTimeMillis();
        int ret = worker.doNewOrder(newOrderTxn);
        long end = System.currentTimeMillis();
        Counter.addResponseTime(end - begin);
        if (ret != 0)
            newOrderTxn.printAfterRollback(this.printStream);
        else
            newOrderTxn.printResult(this.printStream);
    }

    public void doPayment() {
//        PaymentTxn paymentTxn = new PaymentTxn(w_id, w_cnt);
//        int ret = this.executor.doPayment(paymentTxn);
//        paymentTxn.printResult(this.printStream);
    }

    public void doStockLevel() {
//        StockLevelTxn stockLevelTxn = new StockLevelTxn(w_id, t_id);
//        int ret = this.executor.doStockLevel(stockLevelTxn);
    }

}
