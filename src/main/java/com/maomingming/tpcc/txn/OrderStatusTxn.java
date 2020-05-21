package com.maomingming.tpcc.txn;

import com.maomingming.tpcc.util.RandomGenerator;

import java.math.BigDecimal;
import java.util.Date;

public class OrderStatusTxn {
    public int w_id;

    public int d_id;
    public int c_id;
    public String c_last;

    public String c_first;
    public String c_middle;
    public BigDecimal c_balance;
    public int o_id;
    public Date o_entry_d;
    public int o_carrier_id;

    public static class OutputRepeatingGroup {
        public int ol_i_id;
        public int ol_supply_w_id;
        public Date ol_delivery_d;
        public int ol_quantity;
        public BigDecimal ol_amount;
        public OutputRepeatingGroup(int ol_i_id, int ol_supply_w_id, int ol_quantity, BigDecimal ol_amount, Date ol_delivery_d) {
            this.ol_i_id = ol_i_id;
            this.ol_supply_w_id = ol_supply_w_id;
            this.ol_quantity = ol_quantity;
            this.ol_amount = ol_amount;
            this.ol_delivery_d = ol_delivery_d;
        }
    }
    public OrderStatusTxn.OutputRepeatingGroup[] outputRepeatingGroups;

    public OrderStatusTxn(int w_id) {
        this.w_id = w_id;
        d_id = RandomGenerator.makeNumber(1, 10);
        if (RandomGenerator.makeNumber(1, 100) <= 60) {
            c_last = RandomGenerator.makeLastNameForRun();
            c_id = 0;
        } else {
            c_id = RandomGenerator.makeNURand(1023, 1, 3000);
            c_last = null;
        }
    }
}
