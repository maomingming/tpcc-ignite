package com.maomingming.tpcc.txn;

import com.maomingming.tpcc.record.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;

public class KeyValueExecutor implements Executor{
    int w_id;
    Ignite ignite;
    IgniteCache<String, CustRecord> custCache;
    IgniteCache<String, DistRecord> distCache;
    IgniteCache<String, HistRecord> histCache;
    IgniteCache<String, ItemRecord> itemCache;
    IgniteCache<String, NewOrdRecord> newOrdCache;
    IgniteCache<String, OrdLineRecord> ordLineCache;
    IgniteCache<String, OrdRecord> ordCache;
    IgniteCache<String, StockRecord> stockCache;
    IgniteCache<String, WareRecord> wareCache;

    public KeyValueExecutor(int w_id) {
        this.w_id = w_id;
        Ignition.setClientMode(true);
        IgniteConfiguration conf = new IgniteConfiguration();
        conf.setIgniteInstanceName("CLIENT");
        this.ignite = Ignition.getOrStart(conf);
        this.custCache = this.ignite.getOrCreateCache("CUSTOMER");
        this.distCache = this.ignite.getOrCreateCache("DISTRICT");
        this.histCache = this.ignite.getOrCreateCache("HISTORY");
        this.itemCache = this.ignite.getOrCreateCache("ITEM");
        this.newOrdCache = this.ignite.getOrCreateCache("NEW-ORDER");
        this.ordLineCache = this.ignite.getOrCreateCache("ORDER-LINE");
        this.ordCache = this.ignite.getOrCreateCache("ORDER");
        this.stockCache = this.ignite.getOrCreateCache("STOCK");
        this.wareCache = this.ignite.getOrCreateCache("WAREHOUSE");
    }

    public void doNewOrder(int w_id, int d_id, int c_id, int ol_cnt, OrdLineRecord[] ordLineRecords) {
        try (Transaction tx = this.ignite.transactions().txStart()) {
            DistRecord dist = this.distCache.get(DistRecord.getKey(w_id, d_id));
            dist.d_next_o_id += 1;
            this.distCache.put(dist.getKey(), dist);
            tx.commit();
        }
    }

    public void executeFinish() {
        this.custCache.close();
        this.distCache.close();
        this.histCache.close();
        this.itemCache.close();
        this.newOrdCache.close();
        this.ordLineCache.close();
        this.ordCache.close();
        this.stockCache.close();
        this.wareCache.close();
        this.ignite.close();
    }
}
