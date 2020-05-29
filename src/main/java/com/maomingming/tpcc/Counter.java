package com.maomingming.tpcc;

import com.google.common.collect.ImmutableMap;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Counter extends Thread {
    final static Map<String, Integer> MAX_RT = ImmutableMap.of(
            "NEW_ORDER", 50, "PAYMENT", 50, "ORDER_STATUS", 50, "DELIVERY", 800, "STOCK_LEVEL", 200);
    final static int I_NUM_PER_SEC = 10;
    static AtomicInteger newOrderCnt = new AtomicInteger(0);
    static AtomicInteger paymentCnt = new AtomicInteger(0);
    static AtomicInteger orderStatusCnt = new AtomicInteger(0);
    static AtomicInteger deliveryCnt = new AtomicInteger(0);
    static AtomicInteger stockLevelCnt = new AtomicInteger(0);

    static AtomicInteger newOrderPassCnt = new AtomicInteger(0);
    static AtomicInteger paymentPassCnt = new AtomicInteger(0);
    static AtomicInteger orderStatusPassCnt = new AtomicInteger(0);
    static AtomicInteger deliveryPassCnt = new AtomicInteger(0);
    static AtomicInteger stockLevelPassCnt = new AtomicInteger(0);

    static AtomicInteger newOrderRetryCnt = new AtomicInteger(0);
    static AtomicInteger paymentRetryCnt = new AtomicInteger(0);
    static AtomicInteger deliveryRetryCnt = new AtomicInteger(0);

    static AtomicInteger[] newOrderTime = new AtomicInteger[MAX_RT.get("NEW_ORDER") * I_NUM_PER_SEC];
    static AtomicInteger[] paymentTime = new AtomicInteger[MAX_RT.get("PAYMENT") * I_NUM_PER_SEC];
    static AtomicInteger[] orderStatusTime = new AtomicInteger[MAX_RT.get("ORDER_STATUS") * I_NUM_PER_SEC];
    static AtomicInteger[] deliveryTime = new AtomicInteger[MAX_RT.get("DELIVERY") * I_NUM_PER_SEC];
    static AtomicInteger[] stockLevelTime = new AtomicInteger[MAX_RT.get("STOCK_LEVEL") * I_NUM_PER_SEC];
    static PrintStream printStream;
    static {
        for (int i = 0; i < MAX_RT.get("NEW_ORDER") * I_NUM_PER_SEC; i++) {
            newOrderTime[i] = new AtomicInteger(0);
            paymentTime[i] = new AtomicInteger(0);
            orderStatusTime[i] = new AtomicInteger(0);
        }
        for (int i = 0; i < MAX_RT.get("DELIVERY") * I_NUM_PER_SEC; i++) {
            deliveryTime[i] = new AtomicInteger(0);
        }
        for (int i = 0; i < MAX_RT.get("STOCK_LEVEL") * I_NUM_PER_SEC; i++) {
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
        int init = newOrderCnt.get();
        while (true) {
            try {
                sleep(30000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            printStream.printf("tpmC: %d, 90th response time: %.1f, %.1f, %.1f, %.1f, %.1f, retry times: %d\n",
//                    (int)((newOrderCnt.get()-init)/((float)(System.currentTimeMillis()-begin)/60000)),
//                    getPercentile(newOrderTime, 0.9f), getPercentile(paymentTime, 0.9f), getPercentile(orderStatusTime, 0.9f),
//                    getPercentile(deliveryTime, 0.9f), getPercentile(stockLevelTime, 0.9f), newOrderRetryCnt.get()+paymentRetryCnt.get()+deliveryRetryCnt.get());
            printStream.printf("tpmC: %d\n", (int)((newOrderCnt.get()-init)/((float)(System.currentTimeMillis()-begin)/60000)));
            int newOrderNow = newOrderCnt.get();
            int paymentNow = paymentCnt.get();
            int orderStatusNow = orderStatusCnt.get();
            int deliveryNow = deliveryCnt.get();
            int stockLevelNow = stockLevelCnt.get();
            int total = newOrderNow + paymentNow + orderStatusNow + deliveryNow + stockLevelNow;
            printTxnInfo("NEW_ORDER", newOrderNow, total, newOrderTime, newOrderRetryCnt.get(),newOrderPassCnt.get());
            printTxnInfo("PAYMENT", paymentNow, total, paymentTime, paymentRetryCnt.get(), paymentPassCnt.get());
            printTxnInfo("ORDER_STATUS", orderStatusNow, total, orderStatusTime, 0, orderStatusPassCnt.get());
            printTxnInfo("DELIVERY", deliveryNow, total, deliveryTime, deliveryRetryCnt.get(), deliveryPassCnt.get());
            printTxnInfo("STOCK_LEVEL", stockLevelNow, total, stockLevelTime, 0, stockLevelPassCnt.get());
            printStream.println();
        }
    }

    public static void printTxnInfo(String name, int cnt, int cntAll, AtomicInteger[] timeCnt, int retry, int pass) {
        printStream.println(name+" Transactions:");
        printStream.printf("total: %d, mix: %.2f%%\n", cnt, (float)cnt/cntAll*100);
        printStream.printf("retry times: %d, retry percentage: %.2f%%, pass: %d, pass percentage: %.2f%%\n",
                retry, (float)retry/cnt*100, pass, (float)pass/cnt*100);
        printStream.printf("Response Time (50th, 75th, 90th, 99th): %.1fs, %.1fs, %.1fs, %.1fs\n",
                getPercentile(timeCnt, name,0.5f), getPercentile(timeCnt, name,0.75f), getPercentile(timeCnt, name,0.9f), getPercentile(timeCnt, name,0.99f));
    }

    public static void addResponseTime(long responseTime, String txnType) {
        int intervalNum = (int)(responseTime * I_NUM_PER_SEC / 1000);
        if (intervalNum >= MAX_RT.get(txnType) * I_NUM_PER_SEC)
            intervalNum = MAX_RT.get(txnType) * I_NUM_PER_SEC - 1;
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

    public static float getPercentile(AtomicInteger[] timeCnt, String type, float p) {
        int[] copy = new int[MAX_RT.get(type) * I_NUM_PER_SEC];
        int total = 0;
        for (int i = 0; i < MAX_RT.get(type) * I_NUM_PER_SEC; i++) {
            copy[i] = timeCnt[i].get();
            total += copy[i];
        }
        int pth = (int) (total * p);
        for (int i = 0; i < MAX_RT.get(type) * I_NUM_PER_SEC; i++) {
            pth -= copy[i];
            if (pth <= 0) {
                return (float)i/I_NUM_PER_SEC;
            }
        }
        return MAX_RT.get(type);
    }
}
