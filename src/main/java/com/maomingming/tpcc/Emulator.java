package com.maomingming.tpcc;

import com.maomingming.tpcc.txn.*;
import com.maomingming.tpcc.util.RandomGenerator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.concurrent.*;

public class Emulator extends Thread {
    static int MAX_RETRY_TIMES = 10;

    int w_id;
    int t_id;
    int w_cnt;
    PrintStream printStream;
    Worker worker;
    ExecutorService executorService;

    public void run() {
        key(RandomGenerator.makeNumber(1,100000));
        for (int i = 0; i < 1000000; i++) {
            int r = RandomGenerator.makeNumber(1, 23);
            if (r <= 10) {
                NewOrderTxn newOrderTxn = new NewOrderTxn(w_id, w_cnt);
                key(18000);
                long begin = System.currentTimeMillis();
                int ret = doNext("NEW_ORDER", newOrderTxn, 0);
                if (ret>=0) {
                    Counter.addResponseTime(System.currentTimeMillis() - begin, "NEW_ORDER");
                    Counter.newOrderCnt.incrementAndGet();
                    Counter.newOrderRetryCnt.addAndGet(ret);
                }
                think(12000);
            } else if (r <= 20) {
                PaymentTxn paymentTxn = new PaymentTxn(w_id, w_cnt);
                key(3000);
                long begin = System.currentTimeMillis();
                int ret = doNext("PAYMENT", paymentTxn, 0);
                if (ret>=0) {
                    Counter.addResponseTime(System.currentTimeMillis() - begin, "PAYMENT");
                    Counter.paymentCnt.incrementAndGet();
                    Counter.paymentRetryCnt.addAndGet(ret);
                }
                think(12000);
            } else if (r == 21) {
                OrderStatusTxn orderStatusTxn = new OrderStatusTxn(w_id);
                key(2000);
                long begin = System.currentTimeMillis();
                int ret = doNext("ORDER_STATUS", orderStatusTxn, 0);
                if (ret>=0) {
                    Counter.addResponseTime(System.currentTimeMillis() - begin, "ORDER_STATUS");
                    Counter.orderStatusCnt.incrementAndGet();
                }
                think(10000);
            } else if (r == 22) {
                DeliveryTxn deliveryTxn = new DeliveryTxn(w_id);
                key(2000);
                long begin = System.currentTimeMillis();
                int ret = doNext("DELIVERY", deliveryTxn, 0);
                if (ret>=0) {
                    Counter.addResponseTime(System.currentTimeMillis() - begin, "DELIVERY");
                    Counter.deliveryCnt.incrementAndGet();
                    Counter.deliveryRetryCnt.addAndGet(ret);
                }
                think(5000);
            } else {
                StockLevelTxn stockLevelTxn = new StockLevelTxn(w_id, t_id);
                key(2000);
                long begin = System.currentTimeMillis();
                int ret = doNext("STOCK_LEVEL", stockLevelTxn, 0);
                if (ret>=0) {
                    Counter.addResponseTime(System.currentTimeMillis() - begin, "STOCK_LEVEL");
                    Counter.stockLevelCnt.incrementAndGet();
                }
                think(5000);
            }
        }
        worker.finish();
    }

    void think(int mean_time) {
        int time = (int) (-Math.log(RandomGenerator.makeDecimal(1, 99, 2).doubleValue()) * mean_time);
        if (time > mean_time *10)
            time = mean_time*10;
//        System.out.printf("w_id: %d, t_id: %d, time: %d\n",w_id, t_id,time);
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
        executorService = Executors.newSingleThreadExecutor();
    }

    public int doNext(String type, Txn txn, int times) {
        FutureTask<Integer> futureTask = new FutureTask<>(() -> {
            switch (type) {
                case "NEW_ORDER":
                    return worker.doNewOrder((NewOrderTxn) txn);
                case "PAYMENT":
                    return worker.doPayment((PaymentTxn) txn);
                case "ORDER_STATUS":
                    return worker.doOrderStatus((OrderStatusTxn) txn);
                case "DELIVERY":
                    return worker.doDelivery((DeliveryTxn) txn);
                case "STOCK_LEVEL":
                    return worker.doStockLevel((StockLevelTxn) txn);
                default:
                    return 0;
            }
        });
        executorService.execute(futureTask);
        try {
            Integer ret = futureTask.get(50000, TimeUnit.MILLISECONDS);
            if (ret==0) txn.printResult(printStream);
            return times;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            futureTask.cancel(true);
            worker.rollback();
            if (times<MAX_RETRY_TIMES)
                return doNext(type, txn, times+1);
        }
        return -1;
    }

//    public void doNewOrder() {
//        NewOrderTxn newOrderTxn = new NewOrderTxn(w_id, w_cnt);
//        key(18000);
//        long begin = System.currentTimeMillis();
//        Integer ret = null;
//        for (int i = 0; i < MAX_RETRY_TIMES && ret == null; i++) {
//            try {
//                ret = worker.doNewOrder(newOrderTxn);
//            } catch (TransactionRetryException e) {
//                System.out.printf("retry times: %d\n", i+1);
//                Counter.newOrderRetryCnt.incrementAndGet();
//                key(1000);
//            }
//        }
//        if (ret == null)
//            return;
//        long end = System.currentTimeMillis();
//        Counter.newOrderCnt.incrementAndGet();
//        Counter.addResponseTime(end - begin, "NEW_ORDER");
//        if (ret != 0)
//            newOrderTxn.printAfterRollback(this.printStream);
//        else
//            newOrderTxn.printResult(this.printStream);
//    }
//
//    public void doPayment() {
//        PaymentTxn paymentTxn = new PaymentTxn(w_id, w_cnt);
//        key(3000);
//        long begin = System.currentTimeMillis();
//        Integer ret = null;
//        for (int i = 0; i < MAX_RETRY_TIMES && ret == null; i++) {
//            try {
//                ret = worker.doPayment(paymentTxn);
//            } catch (TransactionRetryException e) {
//                System.out.printf("retry times: %d\n", i+1);
//                Counter.paymentRetryCnt.incrementAndGet();
//                key(1000);
//            }
//        }
//        if (ret == null)
//            return;
//        long end = System.currentTimeMillis();
//        Counter.paymentCnt.incrementAndGet();
//        Counter.addResponseTime(end - begin, "PAYMENT");
//        paymentTxn.printResult(this.printStream);
//    }
//
//    public void doOrderStatus() {
//        OrderStatusTxn orderStatusTxn = new OrderStatusTxn(w_id);
//        key(2000);
//        long begin = System.currentTimeMillis();
//        int ret = worker.doOrderStatus(orderStatusTxn);
//        long end = System.currentTimeMillis();
//        Counter.orderStatusCnt.incrementAndGet();
//        Counter.addResponseTime(end - begin, "ORDER_STATUS");
//        orderStatusTxn.printResult(printStream);
//    }
//
//    public void doDelivery() {
//        DeliveryTxn deliveryTxn = new DeliveryTxn(w_id);
//        key(2000);
//        long begin = System.currentTimeMillis();
//        Integer ret = null;
//        for (int i = 0; i < MAX_RETRY_TIMES && ret == null; i++) {
//            try {
//                ret = worker.doDelivery(deliveryTxn);
//            } catch (TransactionRetryException e) {
//                System.out.printf("retry times: %d\n", i+1);
//                Counter.deliveryRetryCnt.incrementAndGet();
//                key(1000);
//            }
//        }
//        if (ret == null)
//            return;
//        long end = System.currentTimeMillis();
//        Counter.deliveryCnt.incrementAndGet();
//        Counter.addResponseTime(end - begin, "DELIVERY");
//        deliveryTxn.printResult(this.printStream);
//    }
//
//    public void doStockLevel() {
//        StockLevelTxn stockLevelTxn = new StockLevelTxn(w_id, t_id);
//        key(2000);
//        long begin = System.currentTimeMillis();
//        int ret = worker.doStockLevel(stockLevelTxn);
//        long end = System.currentTimeMillis();
//        Counter.stockLevelCnt.incrementAndGet();
//        Counter.addResponseTime(end - begin, "STOCK_LEVEL");
//        stockLevelTxn.printResult(printStream);
//    }

}
