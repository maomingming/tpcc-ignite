package com.maomingming.tpcc.load;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;

import com.maomingming.tpcc.record.Record;

import java.util.HashMap;

public class StreamLoader implements Loader{

    Ignite ignite;
    HashMap<String, IgniteCache<String, Record>> caches = new HashMap<>();
    HashMap<String, IgniteDataStreamer<String, Record>> stmrs = new HashMap<>();

    public StreamLoader() {
        Ignition.setClientMode(true);
        this.ignite = Ignition.start();
        for (String table : TABLES) {
            caches.put(table, this.ignite.getOrCreateCache(table));
            stmrs.put(table, this.ignite.dataStreamer(table));
        }
    }

    public void load(String tableName, Record r) {
        this.stmrs.get(tableName).addData(r.getKey(), r);
    }

    public void loadFinish() {
        for (String table : TABLES) {
            this.stmrs.get(table).close();
            this.caches.get(table).close();
        }
        this.ignite.close();
    }
}
