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

    static {
        for (int i = 0; i < MAX_RT * I_NUM_PER_SEC; i++) {
            newOrderTime[i] = new AtomicInteger(0);
            paymentTime[i] = new AtomicInteger(0);
            orderStatusTime[i] = new AtomicInteger(0);
            deliveryTime[i] = new AtomicInteger(0);
            stockLevelTime[i] = new AtomicInteger(0);
        }
    }
    public void run() {
        PrintStream printStream;
        try {
            printStream = new PrintStream("result/stat.log");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
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
            printStream.printf("tpmC: %d, ", (int)((newOrderCnt.get()-last_cnt)/((float)(System.currentTimeMillis()-lastMill)/60000)));
            printStream.printf("90th Percentile Response Time: %.1fs, %.1fs, %.1fs, %.1fs, %.1fs. ",
                    getPercentile(newOrderTime,0.9f), getPercentile(paymentTime,0.9f),
                    getPercentile(orderStatusTime,0.9f), getPercentile(deliveryTime,0.9f),getPercentile(stockLevelTime,0.9f));
            printStream.printf("Retry times: %d, %d, %d\n", newOrderRetryCnt.get()-lastNewOrderRetry,
                    paymentRetryCnt.get()-lastPaymentRetry, deliveryRetryCnt.get()-lastDeliveryRetry);
        }
    }

    public static void addResponseTime(long responseTime, String txnType) {
        int intervalNum = (int)(responseTime * I_NUM_PER_SEC / 1000);
        if (intervalNum > MAX_RT * I_NUM_PER_SEC)
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
