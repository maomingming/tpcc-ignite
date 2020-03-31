package com.maomingming.tpcc.txn;

import com.maomingming.tpcc.RandomGenerator;
import com.maomingming.tpcc.record.*;

import java.io.PrintStream;

public class NewOrder {
    public int w_id;
    public int d_id;
    public int c_id;
    public OrdLineRecord[] ordLineRecords;

    public WareRecord wareRecord;
    public DistRecord distRecord;
    public CustRecord custRecord;
    public OrdRecord ordRecord;
    public float totalAmount;

    public ItemRecord[] itemRecords;
    public StockRecord[] stockRecords;
    public char[] brandGenerics;

    public NewOrder(int w_id, int w_cnt) {
        this.w_id = w_id;
        this.d_id = RandomGenerator.makeNumber(1, 10);
        this.c_id = RandomGenerator.makeNURand(1023, 1, 3000);
        int ol_cnt = RandomGenerator.makeNumber(5, 15);
        int rbk = RandomGenerator.makeNumber(1, 100);

        this.ordLineRecords = new OrdLineRecord[ol_cnt];
        for (int i = 0; i < ol_cnt; i ++) {
            ordLineRecords[i] = new OrdLineRecord(d_id, w_id, i==ol_cnt-1 && rbk==1, w_cnt);
        }
        this.itemRecords = new ItemRecord[ol_cnt];
        this.stockRecords = new StockRecord[ol_cnt];
        this.brandGenerics = new char[ol_cnt];
    }

    public void printResult(PrintStream printStream) {
        printStream.println("NEW ORDER");
        printStream.printf("O_ID: %s%n", ordRecord.o_id);
    }

}
