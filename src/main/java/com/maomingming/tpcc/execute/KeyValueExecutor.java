package com.maomingming.tpcc.execute;

import com.maomingming.tpcc.record.*;
import com.maomingming.tpcc.txn.NewOrder;
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

    public void doNewOrder(NewOrder newOrder) {
        try (Transaction tx = this.ignite.transactions().txStart()) {
            newOrder.wareRecord = this.wareCache.get(WareRecord.getKey(w_id));

            newOrder.distRecord = this.distCache.get(DistRecord.getKey(w_id, newOrder.d_id));
            int o_id = newOrder.distRecord.d_next_o_id;
            newOrder.distRecord.d_next_o_id ++;
            this.distCache.put(newOrder.distRecord.getKey(), newOrder.distRecord);

            newOrder.custRecord = this.custCache.get(CustRecord.getKey(w_id, newOrder.d_id, newOrder.c_id));

            boolean all_local = true;
            for (OrdLineRecord ordLine : newOrder.ordLineRecords) {
                if (ordLine.ol_w_id != ordLine.ol_supply_w_id) {
                    all_local = false;
                    break;
                }
            }
            int ol_cnt = newOrder.ordLineRecords.length;
            newOrder.ordRecord = new OrdRecord(o_id, newOrder.c_id, newOrder.d_id, w_id, ol_cnt, all_local);
            this.ordCache.put(newOrder.ordRecord.getKey(), newOrder.ordRecord);
            NewOrdRecord newOrdRecord =  new NewOrdRecord(o_id, newOrder.d_id, w_id);
            this.newOrdCache.put(newOrdRecord.getKey(), newOrdRecord);

            for (int i = 0; i < ol_cnt; i++) {
                OrdLineRecord ordLineRecord = newOrder.ordLineRecords[i];
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
                stockCache.put(stockRecord.getKey(), stockRecord);

                if (itemRecord.i_data.contains("ORIGINAL") && stockRecord.s_data.contains("ORIGINAL"))
                    newOrder.brandGenerics[i] = 'B';
                else
                    newOrder.brandGenerics[i] = 'G';

                ordLineRecord.ol_o_id = o_id;
                ordLineRecord.ol_number = i + 1;
                ordLineRecord.ol_amount = ordLineRecord.ol_quantity * itemRecord.i_price;
                ordLineRecord.ol_dist_info = stockRecord.getDist(newOrder.d_id);
                ordLineCache.put(ordLineRecord.getKey(), ordLineRecord);

                newOrder.totalAmount += ordLineRecord.ol_amount;

                newOrder.itemRecords[i] = itemRecord;
                newOrder.stockRecords[i] = stockRecord;
            }
            newOrder.totalAmount *= (1 - newOrder.custRecord.c_discount) * (1 + newOrder.wareRecord.w_tax + newOrder.distRecord.d_tax);

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
