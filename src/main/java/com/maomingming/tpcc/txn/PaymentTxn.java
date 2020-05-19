package com.maomingming.tpcc.txn;

import com.maomingming.tpcc.util.RandomGenerator;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.util.Date;

public class PaymentTxn {
    public int w_id;

    public int d_id;
    public int c_id;
    public int c_d_id;
    public int c_w_id;
    public BigDecimal h_amount;
    public String c_last;

    public Date h_date;
    public String w_street_1;
    public String w_street_2;
    public String w_city;
    public String w_state;
    public String w_zip;
    public String d_street_1;
    public String d_street_2;
    public String d_city;
    public String d_state;
    public String d_zip;
    public String c_first;
    public String c_middle;
    public String c_street_1;
    public String c_street_2;
    public String c_city;
    public String c_state;
    public String c_zip;
    public String c_phone;
    public Date c_since;
    public String c_credit;
    public BigDecimal c_credit_lim;
    public BigDecimal c_discount;
    public BigDecimal c_balance;
    public String c_data;

    public PaymentTxn(int w_id, int w_cnt) {
        this.w_id = w_id;
        d_id = RandomGenerator.makeNumber(1, 10);
        if (RandomGenerator.makeNumber(1, 100) <= 60) {
            c_last = RandomGenerator.makeLastNameForRun();
            c_id = 0;
        } else {
            c_id = RandomGenerator.makeNURand(1023, 1, 3000);
            c_last = null;
        }
        if (RandomGenerator.makeNumber(1, 100) <= 85) {
            c_d_id = d_id;
            c_w_id = w_id;
        } else {
            c_d_id = RandomGenerator.makeNumber(1, 10);
            if (w_cnt == 1)
                c_w_id = 1;
            else {
                c_w_id = RandomGenerator.makeNumber(1, w_cnt - 1);
                if (c_w_id >= w_id)
                    c_w_id += 1;
            }
        }
        h_amount = RandomGenerator.makeDecimal(100, 500000, 2);
    }

    public void printResult(PrintStream printStream) {
        printStream.println("Payment");
        printStream.println(h_date);
        printStream.printf("Warehouse: %d\tDistrict: %d\n", w_id, d_id);
        printStream.printf("Customer: %d\tCust-Warehouse: %d\n", c_id, c_w_id);
        printStream.printf("Amount Paid: %f", h_amount);
    }
}
