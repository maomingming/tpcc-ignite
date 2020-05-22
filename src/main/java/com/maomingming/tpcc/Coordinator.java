package com.maomingming.tpcc;

import com.maomingming.tpcc.record.Customer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;

public class Coordinator {

    static int w_cnt = 5;
    static String loaderType = "STREAM_LOADER";
    static String executorType = "KEY_VALUE_EXECUTOR";

    public static void main(String[] args) throws Exception {
        Populator populator = new Populator(loaderType, w_cnt);
        populator.loadAll();

        new Counter().start();
        for (int w_id = 1; w_id <= w_cnt; w_id ++) {
            for (int t_id = 1; t_id <= 10; t_id ++) {
                new Emulator(executorType, w_id, t_id, w_cnt).start();
            }
        }
    }
}
