package com.maomingming.tpcc;

import java.util.concurrent.atomic.AtomicInteger;

public class Counter extends Thread {
    final static int MAX_RT = 10;
    final static int I_NUM_PER_SEC = 10;
    static AtomicInteger cnt = new AtomicInteger(0);
    static AtomicInteger[] responseTimeCnt = new AtomicInteger[MAX_RT * I_NUM_PER_SEC];
    static {
        for (int i = 0; i < MAX_RT * I_NUM_PER_SEC; i++)
            responseTimeCnt[i] = new AtomicInteger(0);
    }
    public void run() {
        while (true) {
            long lastMill = System.currentTimeMillis();
            int last_cnt = cnt.get();
            try {
                sleep(30000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.printf("tpmC: %d, ", (int)((cnt.get()-last_cnt)/((float)(System.currentTimeMillis()-lastMill)/60000)));
            System.out.printf("90th Percentile Response Time: %.1fs\n", (float)getPercentile(0.9f)/I_NUM_PER_SEC);
        }
    }

    public static void addResponseTime(long responseTime) {
        int intervalNum = (int)(responseTime * I_NUM_PER_SEC / 1000);
        if (intervalNum > MAX_RT * I_NUM_PER_SEC)
            intervalNum = MAX_RT * I_NUM_PER_SEC - 1;
        responseTimeCnt[intervalNum].incrementAndGet();
    }

    public static int getPercentile(float p) {
        int[] copy = new int[MAX_RT * I_NUM_PER_SEC];
        int total = 0;
        for (int i = 0; i < MAX_RT * I_NUM_PER_SEC; i++) {
            copy[i] = responseTimeCnt[i].get();
            total += copy[i];
        }
        int pth = (int) (total * p);
        for (int i = 0; i < MAX_RT * I_NUM_PER_SEC; i++) {
            pth -= copy[i];
            if (pth <= 0) {
                return i;
            }
        }
        return MAX_RT * I_NUM_PER_SEC;
    }
}
