package com.maomingming.tpcc;

import com.maomingming.tpcc.load.Loader;
import com.maomingming.tpcc.load.SQLLoader;
import com.maomingming.tpcc.load.StreamLoader;
import com.maomingming.tpcc.record.*;
import com.maomingming.tpcc.record.Warehouse;
import com.maomingming.tpcc.util.RandomGenerator;

import java.util.ArrayList;

public class Populator {

    Loader loader;
    int w_cnt;

    public Populator(String loaderType, int w_cnt) {
        this.loader = getLoader(loaderType);
        this.w_cnt = w_cnt;
    }

    public void loadAll() throws Exception {
        loader.loadBegin();
//        for (int i_id = 1; i_id <= 100000; i_id++) {
//            loader.load("ITEM", new Item(i_id));
//        }
        for (int w_id = 1; w_id <= this.w_cnt; w_id++) {
//            loader.load("WAREHOUSE", new Warehouse(w_id));
//            for (int s_i_id = 1; s_i_id <= 100000; s_i_id++) {
//                loader.load("STOCK", new Stock(s_i_id, w_id));
//            }
            for (int d_id = 1; d_id <= 10; d_id++) {
//                loader.load("DISTRICT", new District(d_id, w_id));
                for (int c_id = 1; c_id <= 3000; c_id++) {
                    loader.load("CUSTOMER", new Customer(c_id, d_id, w_id));
//                    loader.load("HISTORY", new History(c_id, d_id, w_id));
                }
//                ArrayList<Integer> perm = RandomGenerator.makePermutation(3000);
//                for (int o_id = 1; o_id <= 3000; o_id++) {
//                    Order order = new Order(o_id, perm.get(o_id-1), d_id, w_id);
//                    loader.load("ORDER", order);
//                    OrderLine[] orderLines = order.makeOrdLineForLoad();
//                    for (OrderLine orderLine : orderLines) {
//                        loader.load("ORDER_LINE", orderLine);
//                    }
//                    if (o_id > 2100)
//                        loader.load("NEW_ORDER", new NewOrder(o_id, d_id, w_id));
//                }
            }
        }
        loader.loadFinish();
    }

    private Loader getLoader(String loaderType) {
        switch (loaderType) {
            case "STREAM_LOADER":
                return new StreamLoader();
            case "SQL_LOADER":
                return new SQLLoader();
            default:
                throw new IllegalStateException("Unexpected value: " + loaderType);
        }
    }

}
