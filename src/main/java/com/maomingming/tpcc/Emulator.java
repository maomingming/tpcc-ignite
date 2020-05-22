package com.maomingming.tpcc;

import com.maomingming.tpcc.txn.*;
import com.maomingming.tpcc.util.RandomGenerator;

import java.io.FileNotFoundException;
import java.io.PrintStream;

public class Emulator extends Thread {
    static int MAX_RETRY_TIMES = 10;

    int w_id;
    int t_id;
    int w_cnt;
    PrintStream printStream;
    Worker worker;

    public void run() {
        for (int i = 0; i < 1000000; i++) {
            doNext();
        }
        worker.finish();
    }

    void think(int mean_time) {
        int time = (int)(-Math.log(RandomGenerator.makeDecimal(0, 99, 2).doubleValue())*mean_time);
        key(time);
    }

    void key(int time) {
        try {
            sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Emulator(String executorType, int w_id, int t_id, int w_cnt) throws Exception {
        this.w_id = w_id;
        this.t_id = t_id;
        this.w_cnt = w_cnt;
        try {
            this.printStream = new PrintStream("result/" + w_id + "." + t_id + ".log");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        worker = new Worker(executorType, w_id);
    }

    public void doNext() {
        int r = RandomGenerator.makeNumber(1, 23);
        if (r <= 10)
            doNewOrder();
        else if (r <= 20)
            doPayment();
        else if (r == 21)
            doOrderStatus();
        else if (r == 22)
            doDelivery();
        else
            doStockLevel();
    }

    public void doNewOrder() {
        think(12000);
        NewOrderTxn newOrderTxn = new NewOrderTxn(w_id, w_cnt);
        key(18000);
        long begin = System.currentTimeMillis();
        Integer ret = null;
        for (int i = 0; i < MAX_RETRY_TIMES && ret == null; i++) {
            try {
                ret = worker.doNewOrder(newOrderTxn);
            } catch (TransactionRetryException e) {
                System.out.printf("retry times: %d\n", i);
            }
        }
        if (ret == null)
            return;
        long end = System.currentTimeMillis();
        Counter.cnt.incrementAndGet();
        Counter.addResponseTime(end - begin);
        if (ret != 0)
            newOrderTxn.printAfterRollback(this.printStream);
        else
            newOrderTxn.printResult(this.printStream);
    }

    public void doPayment() {
        think(12000);
        PaymentTxn paymentTxn = new PaymentTxn(w_id, w_cnt);
        key(3000);
        Integer ret = null;
        for (int i = 0; i < MAX_RETRY_TIMES && ret == null; i++) {
            try {
                ret = worker.doPayment(paymentTxn);
            } catch (TransactionRetryException e) {
                System.out.printf("retry times: %d\n", i);
            }
        }
        if (ret != null && ret == 0)
            paymentTxn.printResult(this.printStream);
    }

    public void doOrderStatus() {
        think(10000);
        OrderStatusTxn orderStatusTxn = new OrderStatusTxn(w_id);
        key(2000);
        int ret = worker.doOrderStatus(orderStatusTxn);
        if (ret == 0)
            orderStatusTxn.printResult(printStream);
    }

    public void doDelivery() {
        think(5000);
        DeliveryTxn deliveryTxn = new DeliveryTxn(w_id);
        key(2000);
        Integer ret = null;
        for (int i = 0; i < MAX_RETRY_TIMES && ret == null; i++) {
            try {
                ret = worker.doDelivery(deliveryTxn);
            } catch (TransactionRetryException e) {
                System.out.printf("retry times: %d\n", i);
            }
        }
        if (ret != null && ret == 0)
            deliveryTxn.printResult(printStream);
    }

    public void doStockLevel() {
        think(5000);
        StockLevelTxn stockLevelTxn = new StockLevelTxn(w_id, t_id);
        key(2000);
        int ret = worker.doStockLevel(stockLevelTxn);
        if (ret == 0)
            stockLevelTxn.printResult(printStream);
    }

}
