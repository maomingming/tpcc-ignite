package com.maomingming.tpcc;

import com.maomingming.tpcc.driver.Driver;
import com.maomingming.tpcc.driver.DriverFactory;
import com.maomingming.tpcc.driver.KeyValueDriver;
import com.maomingming.tpcc.driver.SQLDriver;
import com.maomingming.tpcc.record.*;
import com.maomingming.tpcc.util.RandomGenerator;

import java.util.ArrayList;
import java.util.List;

public class Populator extends Thread {

    Driver driver;
    List<Integer> w_ids;
    boolean loadItem;

    public Populator(String driverType, List<Integer> w_ids, boolean loadItem) throws Exception {
        driver = DriverFactory.getDriver(driverType);
        driver.loadStart();
        this.w_ids = w_ids;
        this.loadItem = loadItem;
    }

    public void run() {
        if (loadItem)
            for (int i_id = 1; i_id <= 100000; i_id++) {
                driver.load("ITEM", new Item(i_id));
            }
        for (int w_id : w_ids) {
            System.out.println(w_id);
            driver.load("WAREHOUSE", new Warehouse(w_id));
            for (int s_i_id = 1; s_i_id <= 100000; s_i_id++) {
                driver.load("STOCK", new Stock(s_i_id, w_id));
            }
            for (int d_id = 1; d_id <= 10; d_id++) {
                driver.load("DISTRICT", new District(d_id, w_id));
                for (int c_id = 1; c_id <= 3000; c_id++) {
                    driver.load("CUSTOMER", new Customer(c_id, d_id, w_id));
                    driver.load("HISTORY", new History(c_id, d_id, w_id));
                }
                ArrayList<Integer> perm = RandomGenerator.makePermutation(3000);
                for (int o_id = 1; o_id <= 3000; o_id++) {
                    Order order = new Order(o_id, d_id, w_id, perm.get(o_id-1));
                    driver.load("ORDER", order);
                    OrderLine[] orderLines = order.makeOrdLineForLoad();
                    for (OrderLine orderLine : orderLines) {
                        driver.load("ORDER_LINE", orderLine);
                    }
                    if (o_id > 2100)
                        driver.load("NEW_ORDER", new NewOrder(o_id, d_id, w_id));
                }
            }
        }
        driver.loadFinish();
    }

}
