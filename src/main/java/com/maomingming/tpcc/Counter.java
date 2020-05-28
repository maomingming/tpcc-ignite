package com.maomingming.tpcc;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicInteger;

public class Counter extends Thread {
    final static int MAX_RT = 30;
    final static int I_NUM_PER_SEC = 10;
    static AtomicInteger newOrderCnt = new AtomicInteger(0);
    static AtomicInteger paymentCnt = new AtomicInteger(0);
    static AtomicInteger orderStatusCnt = new AtomicInteger(0);
    static AtomicInteger deliveryCnt = new AtomicInteger(0);
    static AtomicInteger stockLevelCnt = new AtomicInteger(0);

    static AtomicInteger newOrderRetryCnt = new AtomicInteger(0);
    static AtomicInteger paymentRetryCnt = new AtomicInteger(0);
    static AtomicInteger deliveryRetryCnt = new AtomicInteger(0);

    static AtomicInteger[] newOrderTime = new AtomicInteger[MAX_RT * I_NUM_PER_SEC];
    static AtomicInteger[] paymentTime = new AtomicInteger[MAX_RT * I_NUM_PER_SEC];
    static AtomicInteger[] orderStatusTime = new AtomicInteger[MAX_RT * I_NUM_PER_SEC];
    static AtomicInteger[] deliveryTime = new AtomicInteger[MAX_RT * I_NUM_PER_SEC];
    static AtomicInteger[] stockLevelTime = new AtomicInteger[MAX_RT * I_NUM_PER_SEC];
    static PrintStream printStream;
    static {
        for (int i = 0; i < MAX_RT * I_NUM_PER_SEC; i++) {
            newOrderTime[i] = new AtomicInteger(0);
            paymentTime[i] = new AtomicInteger(0);
            orderStatusTime[i] = new AtomicInteger(0);
            deliveryTime[i] = new AtomicInteger(0);
            stockLevelTime[i] = new AtomicInteger(0);
        }
        try {
            printStream = new PrintStream("result/stat.log");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
    public void run() {
        try {
            sleep(120000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long begin = System.currentTimeMillis();
        while (true) {
            long lastMill = System.currentTimeMillis();
            int last_cnt = newOrderCnt.get();
            int lastNewOrderRetry = newOrderRetryCnt.get();
            int lastPaymentRetry = paymentRetryCnt.get();
            int lastDeliveryRetry = deliveryRetryCnt.get();
            try {
                sleep(30000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            printStream.printf("tpmC: %d\n", (int)(newOrderCnt.get()/((float)(System.currentTimeMillis()-begin)/60000)));
            int newOrderNow = newOrderCnt.get();
            int paymentNow = paymentCnt.get();
            int orderStatusNow = orderStatusCnt.get();
            int deliveryNow = deliveryCnt.get();
            int stockLevelNow = stockLevelCnt.get();
            int total = newOrderNow + paymentNow + orderStatusNow + deliveryNow + stockLevelNow;
            printTxnInfo("NEW-ORDER", newOrderNow, total, newOrderTime, newOrderRetryCnt.get());
            printTxnInfo("PAYMENT", paymentNow, total, paymentTime, paymentRetryCnt.get());
            printTxnInfo("ORDER-STATUS", orderStatusNow, total, orderStatusTime, 0);
            printTxnInfo("DELIVERY", deliveryNow, total, deliveryTime, deliveryRetryCnt.get());
            printTxnInfo("STOCK-LEVEL", stockLevelNow, total, stockLevelTime, 0);
            printStream.println();
        }
    }

    public static void printTxnInfo(String name, int cnt, int cntAll, AtomicInteger[] timeCnt, int retry) {
        printStream.println(name+" Transactions:");
        printStream.printf("total: %d, mix: %.2f%%\n", cnt, (float)cnt/cntAll);
        printStream.printf("retry times: %d, retry percentage: %.2f%%\n", retry, (float)retry/cnt);
        printStream.printf("Response Time (50th, 75th, 90th, 99th): %.1fs, %.1fs, %.1fs, %.1fs\n",
                getPercentile(timeCnt, 0.5f), getPercentile(timeCnt, 0.75f), getPercentile(timeCnt, 0.9f), getPercentile(timeCnt, 0.99f));
    }

    public static void addResponseTime(long responseTime, String txnType) {
        int intervalNum = (int)(responseTime * I_NUM_PER_SEC / 1000);
        if (intervalNum >= MAX_RT * I_NUM_PER_SEC)
            intervalNum = MAX_RT * I_NUM_PER_SEC - 1;
        switch (txnType) {
            case "NEW_ORDER":
                newOrderTime[intervalNum].incrementAndGet();
            case "PAYMENT":
                paymentTime[intervalNum].incrementAndGet();
            case "ORDER_STATUS":
                orderStatusTime[intervalNum].incrementAndGet();
            case "DELIVERY":
                deliveryTime[intervalNum].incrementAndGet();
            case "STOCK_LEVEL":
                stockLevelTime[intervalNum].incrementAndGet();
        }
    }

    public static float getPercentile(AtomicInteger[] timeCnt, float p) {
        int[] copy = new int[MAX_RT * I_NUM_PER_SEC];
        int total = 0;
        for (int i = 0; i < MAX_RT * I_NUM_PER_SEC; i++) {
            copy[i] = timeCnt[i].get();
            total += copy[i];
        }
        int pth = (int) (total * p);
        for (int i = 0; i < MAX_RT * I_NUM_PER_SEC; i++) {
            pth -= copy[i];
            if (pth <= 0) {
                return (float)i/I_NUM_PER_SEC;
            }
        }
        return MAX_RT;
    }
}
