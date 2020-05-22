package com.maomingming.tpcc.txn;

import com.maomingming.tpcc.util.RandomGenerator;

import java.io.PrintStream;

public class StockLevelTxn {
    public int w_id;
    public int d_id;
    public int threshold;
    public int low_stock;

    public StockLevelTxn(int w_id, int t_id) {
        this.w_id = w_id;
        d_id = t_id;
        threshold = RandomGenerator.makeNumber(10, 20);
    }

    public void printResult(PrintStream printStream) {
        printStream.println("Stock-Level");
        printStream.printf("Warehouse: %s\tDistrict: %s\n", w_id, d_id);
        printStream.printf("Stock Level Threshold: %s\n", threshold);
        printStream.printf("low stock: %s\n", low_stock);
        printStream.println();
    }
}
