package com.maomingming.tpcc;

import com.maomingming.tpcc.record.DistRecord;
import com.maomingming.tpcc.record.OrdRecord;
import com.maomingming.tpcc.record.Record;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;

public class Coordinator {

    static int w_cnt = 10;
    static int t_cnt_per_w = 10;
    static String loaderType = "STREAM_LOADER";
    static String executorType = "KEY_VALUE_EXECUTOR";

    public static void main(String[] args) {
//        Populator initializer = new Populator(loaderType, w_cnt);
//        initializer.loadAll();
        new CheckPoint().start();
        for (int i = 1; i <= w_cnt; i ++) {
            for (int j = 0; j < t_cnt_per_w; j++) {
                new Emulator(executorType, i, j, w_cnt).start();
            }
        }
    }
}
