package com.maomingming.tpcc.load;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;

import com.maomingming.tpcc.record.Record;

public class StreamLoader implements Loader{

    Ignite ignite;
    IgniteCache<String, Record> cache;
    IgniteDataStreamer<String, Record> stmr;

    public StreamLoader(String tableName) {
        Ignition.setClientMode(true);
        this.ignite = Ignition.start();
        this.cache = this.ignite.getOrCreateCache(tableName);
        this.stmr = this.ignite.dataStreamer(tableName);
    }

    public void load(Record r) {
        this.stmr.addData(r.getKey(), r);
    }

    public void loadFinish() {
        this.stmr.close();
        this.cache.close();
        this.ignite.close();
    }
}
