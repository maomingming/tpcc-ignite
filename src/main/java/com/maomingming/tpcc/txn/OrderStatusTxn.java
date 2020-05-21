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

    public class OutputRepeatingGroup {
        public int ol_i_id;
        public int ol_supply_w_id;
        public Date ol_delivery_d;
        public int ol_quantity;
        public float ol_amount;
    }
    public NewOrderTxn.OutputRepeatingGroup[] outputRepeatingGroups;

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
