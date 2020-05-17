package com.maomingming.tpcc.txn;

import com.maomingming.tpcc.util.RandomGenerator;

import java.io.PrintStream;
import java.util.Date;

public class NewOrderTxn {
    public int w_id;

    public int d_id;
    public int c_id;

    public class InputRepeatingGroup {
        public int ol_supply_w_id;
        public int ol_i_id;
        public int ol_quantity;

        public InputRepeatingGroup(boolean rbk, int w_cnt) {
            ol_i_id = RandomGenerator.makeNURand(8191, 1, 100000);
            if (rbk)
                ol_i_id = 0;
            if (RandomGenerator.makeNumber(1, 100) > 1)
                ol_supply_w_id = w_id;
            else
                do {
                    ol_supply_w_id = RandomGenerator.makeNumber(1, w_cnt);
                } while (ol_supply_w_id != w_id);
            ol_quantity = RandomGenerator.makeNumber(1, 10);
        }
    }
    public InputRepeatingGroup[] inputRepeatingGroups;

    public String c_last;
    public String c_credit;
    public float c_discount;
    public float w_tax;
    public float d_tax;
    public int o_ol_cnt;
    public int o_id;
    public Date o_entry_d;
    public float totalAmount;

    public class OutputRepeatingGroup {
        public String i_name;
        public int s_quantity;
        public char brand_generic;
        public float i_price;
        public float ol_amount;
    }
    public OutputRepeatingGroup[] outputRepeatingGroups;

    public NewOrderTxn(int w_id, int w_cnt) {
        this.w_id = w_id;
        this.d_id = RandomGenerator.makeNumber(1, 10);
        this.c_id = RandomGenerator.makeNURand(1023, 1, 3000);
        int ol_cnt = RandomGenerator.makeNumber(5, 15);
        int rbk = RandomGenerator.makeNumber(1, 100);

        inputRepeatingGroups = new InputRepeatingGroup[ol_cnt];
        outputRepeatingGroups = new OutputRepeatingGroup[ol_cnt];
        for (int i = 0; i < ol_cnt; i ++) {
            inputRepeatingGroups[i] = new InputRepeatingGroup(i==ol_cnt-1 && rbk==1, w_cnt);
            outputRepeatingGroups[i] = new OutputRepeatingGroup();
        }
    }

    public void printResult(PrintStream printStream) {
        printStream.println("NEW ORDER");
        printStream.printf("Warehouse: %d\tDistrict: %d\tDate: %s\n", w_id, d_id, o_entry_d);
        printStream.printf("Customer: %d\tName: %s\tCredit: %s\tDiscount: %f\n", c_id, c_last, c_credit, c_discount);
        printStream.printf("Order Number: %d\tNumber of Lines: %d\tTax_W: %f\tTax_D: %f\n", o_id, o_ol_cnt, w_tax,d_tax);
        printStream.print("Supp_W\tItem_Id\tItem_Name\tQuantity\tStock_Quantity\tbrand-generic\tPrice\tAmount\n");
        for (int i = 0; i < o_ol_cnt; i++) {
            InputRepeatingGroup input = inputRepeatingGroups[i];
            OutputRepeatingGroup output = outputRepeatingGroups[i];
            printStream.printf("%s\t%s\t%s\t%s\t%s\t%s\t%.2f\t%.2f\n", input.ol_supply_w_id, input.ol_i_id, output.i_name,
                    input.ol_quantity, output.s_quantity, output.brand_generic, output.i_price, output.ol_amount);
        }
    }

    public void printAfterRollback(PrintStream printStream) {
        printStream.println("New Order");
        printStream.printf("Warehouse: %d\tDistrict: %d\n", w_id, d_id);
        printStream.printf("Customer: %d\tName: %s\tCredit: %s\n", c_id, c_last, c_credit);
        printStream.printf("Order Number: %d\n", o_id);
        printStream.println("Item number is not valid");
    }
}
