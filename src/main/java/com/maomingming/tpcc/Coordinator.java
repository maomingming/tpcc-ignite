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

    static int w_cnt = 2;
    static int t_cnt_per_w = 2;
    static String loaderType = "STREAM_LOADER";
    static String executorType = "KEY_VALUE_EXECUTOR";

    public static void main(String[] args) {
        Populator initializer = new Populator(loaderType, w_cnt);
        initializer.loadAll();
        Ignition.setClientMode(true);
        try (Ignite ignite=Ignition.start("config/transaction.xml")) {
            try (IgniteCache<String, Record> cache=ignite.getOrCreateCache("DISTRICT")) {
                DistRecord distRecord = (DistRecord) cache.get(DistRecord.getKey(1, 1));
                System.out.println(distRecord.d_next_o_id);
            }
        }
        for (int i = 1; i <= w_cnt; i ++) {
            for (int j = 0; j < t_cnt_per_w; j++) {
                new Emulator(executorType, i, w_cnt).start();
            }
        }
        try (Ignite ignite=Ignition.start("config/transaction.xml")) {
            try (IgniteCache<String, Record> cache=ignite.getOrCreateCache("DISTRICT")) {
                DistRecord distRecord = (DistRecord) cache.get(DistRecord.getKey(1, 1));
                System.out.println(distRecord.d_next_o_id);
            }
        }
    }
}
