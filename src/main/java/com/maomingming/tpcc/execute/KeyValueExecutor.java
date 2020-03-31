package com.maomingming.tpcc.execute;

import com.maomingming.tpcc.record.*;
import com.maomingming.tpcc.txn.NewOrder;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;

import java.util.Date;

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
            WareRecord wareRecord = this.wareCache.get(WareRecord.getKey(w_id));
            newOrder.w_tax = wareRecord.w_tax;

            DistRecord distRecord = this.distCache.get(DistRecord.getKey(w_id, newOrder.d_id));
            newOrder.d_tax = distRecord.d_tax;
            newOrder.o_id = distRecord.d_next_o_id;
            distRecord.d_next_o_id ++;
            this.distCache.put(distRecord.getKey(), distRecord);

            CustRecord custRecord = this.custCache.get(CustRecord.getKey(w_id, newOrder.d_id, newOrder.c_id));
            newOrder.c_discount = custRecord.c_discount;
            newOrder.c_last = custRecord.c_last;
            newOrder.c_credit = custRecord.c_credit;

            newOrder.o_ol_cnt = newOrder.inputRepeatingGroups.length;

            boolean all_local = true;
            for (int i = 0; i < newOrder.o_ol_cnt; i++) {
                if (newOrder.inputRepeatingGroups[i].ol_supply_w_id != w_id) {
                    all_local = false;
                    break;
                }
            }

            OrdRecord ordRecord = new OrdRecord(newOrder.o_id, newOrder.c_id, newOrder.d_id, w_id, newOrder.o_ol_cnt, all_local);
            this.ordCache.put(ordRecord.getKey(), ordRecord);
            NewOrdRecord newOrdRecord =  new NewOrdRecord(newOrder.o_id, newOrder.d_id, w_id);
            this.newOrdCache.put(newOrdRecord.getKey(), newOrdRecord);

            for (int i = 0; i < newOrder.o_ol_cnt; i++) {
                NewOrder.InputRepeatingGroup input = newOrder.inputRepeatingGroups[i];
                NewOrder.OutputRepeatingGroup output = newOrder.outputRepeatingGroups[i];

                ItemRecord itemRecord = this.itemCache.get(ItemRecord.getKey(input.ol_i_id));
                output.i_price = itemRecord.i_price;
                output.i_name = itemRecord.i_name;

                StockRecord stockRecord = this.stockCache.get(StockRecord.getKey(input.ol_supply_w_id, input.ol_i_id));
                if (stockRecord.s_quantity >= input.ol_quantity + 10)
                    stockRecord.s_quantity -= input.ol_quantity;
                else
                    stockRecord.s_quantity += 91 - input.ol_quantity;
                stockRecord.s_ytd += input.ol_quantity;
                stockRecord.s_order_cnt ++;
                if (input.ol_supply_w_id != w_id)
                    stockRecord.s_remote_cnt ++;
                stockCache.put(stockRecord.getKey(), stockRecord);
                output.s_quantity = stockRecord.s_quantity;

                output.ol_amount = input.ol_quantity * itemRecord.i_price;

                if (itemRecord.i_data.contains("ORIGINAL") && stockRecord.s_data.contains("ORIGINAL"))
                    output.brand_generic = 'B';
                else
                    output.brand_generic = 'G';

                OrdLineRecord ordLineRecord = new OrdLineRecord(newOrder.o_id, newOrder.d_id, w_id,
                        i + 1, input.ol_i_id, input.ol_supply_w_id, input.ol_quantity,
                        output.ol_amount, stockRecord.getDistInfo(newOrder.d_id));
                ordLineCache.put(ordLineRecord.getKey(), ordLineRecord);

                newOrder.totalAmount += output.ol_amount;
            }
            newOrder.totalAmount *= (1 - custRecord.c_discount) * (1 + wareRecord.w_tax + distRecord.d_tax);
            newOrder.o_entry_d = new Date();
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
