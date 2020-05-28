package com.maomingming.tpcc.txn;

import com.maomingming.tpcc.util.RandomGenerator;

import java.io.PrintStream;

public class DeliveryTxn implements Txn {
    public int w_id;
    public int o_carrier_id;

    public int start_d = 1;

    public DeliveryTxn(int w_id) {
        this.w_id = w_id;
        o_carrier_id = RandomGenerator.makeNumber(1, 10);
    }

    public void printResult(PrintStream printStream) {
        printStream.println("Delivery");
        printStream.printf("Warehouse: %s\n", w_id);
        printStream.printf("Carrier Number: %s\n", o_carrier_id);
        printStream.println();
    }
}
