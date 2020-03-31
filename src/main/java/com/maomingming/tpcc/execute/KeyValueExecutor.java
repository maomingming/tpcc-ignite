package com.maomingming.tpcc.execute;

import com.maomingming.tpcc.record.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
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
            float w_tax = this.wareCache.get(WareRecord.getKey(w_id)).w_tax;

            DistRecord dist = this.distCache.get(DistRecord.getKey(w_id, d_id));
            float d_tax = dist.d_tax;
            int o_id = dist.d_next_o_id;
            dist.d_next_o_id ++;
            this.distCache.put(dist.getKey(), dist);

            CustRecord cust = this.custCache.get(CustRecord.getKey(w_id, d_id, c_id));
            float c_discount = cust.c_discount;
            String c_last = cust.c_last;
            String c_credit = cust.c_credit;

            boolean all_local = true;
            for (OrdLineRecord ordLineRecord : ordLineRecords) {
                if (ordLineRecord.ol_w_id != ordLineRecord.ol_supply_w_id) {
                    all_local = false;
                    break;
                }
            }
            OrdRecord ord = new OrdRecord(o_id, c_id, d_id, w_id, ol_cnt, all_local);
            this.ordCache.put(ord.getKey(), ord);
            NewOrdRecord newOrd =  new NewOrdRecord(o_id, d_id, w_id);
            this.newOrdCache.put(newOrd.getKey(), newOrd);

            for (int i = 1; i<= ol_cnt; i++) {
                OrdLineRecord ordLineRecord = ordLineRecords[i-1];
                ItemRecord itemRecord = this.itemCache.get(ItemRecord.getKey(ordLineRecord.ol_i_id));
                StockRecord stockRecord = this.stockCache.get(StockRecord.getKey(ordLineRecord.ol_supply_w_id, ordLineRecord.ol_i_id));
                if (stockRecord.s_quantity >= ordLineRecord.ol_quantity + 10)
                    stockRecord.s_quantity -= ordLineRecord.ol_quantity;
                else
                    stockRecord.s_quantity += 91 - ordLineRecord.ol_quantity;
                stockRecord.s_ytd += ordLineRecord.ol_quantity;
                stockRecord.s_order_cnt ++;
                if (ordLineRecord.ol_w_id != ordLineRecord.ol_supply_w_id)
                    stockRecord.s_remote_cnt ++;
                ordLineRecord.ol_amount = ordLineRecord.ol_quantity * itemRecord.i_price;
//                if (itemRecord.i_data.contains("ORIGINAL") && stockRecord.s_data.contains("ORIGINAL"))
            }

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
