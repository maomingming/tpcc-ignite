package com.maomingming.tpcc.execute;

import com.google.common.collect.ImmutableMap;
import com.maomingming.tpcc.record.*;
import com.maomingming.tpcc.txn.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;

import java.util.Date;
import java.util.List;

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

    public int doNewOrder(NewOrderTxn newOrderTxn) {
        try (Transaction tx = this.ignite.transactions().txStart()) {
            WareRecord wareRecord = this.wareCache.get(WareRecord.getKey(w_id));
            newOrderTxn.w_tax = wareRecord.w_tax;

            DistRecord distRecord = this.distCache.get(DistRecord.getKey(w_id, newOrderTxn.d_id));
            newOrderTxn.d_tax = distRecord.d_tax;
            newOrderTxn.o_id = distRecord.d_next_o_id;
            distRecord.d_next_o_id ++;
            this.distCache.put(distRecord.getKey(), distRecord);

            CustRecord custRecord = this.custCache.get(CustRecord.getKey(w_id, newOrderTxn.d_id, newOrderTxn.c_id));
            newOrderTxn.c_discount = custRecord.c_discount;
            newOrderTxn.c_last = custRecord.c_last;
            newOrderTxn.c_credit = custRecord.c_credit;

            newOrderTxn.o_ol_cnt = newOrderTxn.inputRepeatingGroups.length;

            boolean all_local = true;
            for (int i = 0; i < newOrderTxn.o_ol_cnt; i++) {
                if (newOrderTxn.inputRepeatingGroups[i].ol_supply_w_id != w_id) {
                    all_local = false;
                    break;
                }
            }

            OrdRecord ordRecord = new OrdRecord(newOrderTxn.o_id, newOrderTxn.c_id, newOrderTxn.d_id, w_id, newOrderTxn.o_ol_cnt, all_local);
            this.ordCache.put(ordRecord.getKey(), ordRecord);
            NewOrdRecord newOrdRecord =  new NewOrdRecord(newOrderTxn.o_id, newOrderTxn.d_id, w_id);
            this.newOrdCache.put(newOrdRecord.getKey(), newOrdRecord);

            for (int i = 0; i < newOrderTxn.o_ol_cnt; i++) {
                NewOrderTxn.InputRepeatingGroup input = newOrderTxn.inputRepeatingGroups[i];
                NewOrderTxn.OutputRepeatingGroup output = newOrderTxn.outputRepeatingGroups[i];

                ItemRecord itemRecord = this.itemCache.get(ItemRecord.getKey(input.ol_i_id));
                if (itemRecord == null) {
                    tx.rollback();
                    return -1;
                }
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

                OrdLineRecord ordLineRecord = new OrdLineRecord(newOrderTxn.o_id, newOrderTxn.d_id, w_id,
                        i + 1, input.ol_i_id, input.ol_supply_w_id, input.ol_quantity,
                        output.ol_amount, stockRecord.getDistInfo(newOrderTxn.d_id));
                ordLineCache.put(ordLineRecord.getKey(), ordLineRecord);

                newOrderTxn.totalAmount += output.ol_amount;
            }
            newOrderTxn.totalAmount *= (1 - custRecord.c_discount) * (1 + wareRecord.w_tax + distRecord.d_tax);
            newOrderTxn.o_entry_d = new Date();
            tx.commit();
        }
        return 0;
    }

    @SuppressWarnings("unchecked")
    public int doPayment(PaymentTxn paymentTxn) {
        try (Transaction tx = this.ignite.transactions().txStart()) {
            WareRecord wareRecord = wareCache.get(WareRecord.getKey(w_id));
            String w_name = wareRecord.w_name;
            paymentTxn.w_street_1 = wareRecord.w_street_1;
            paymentTxn.w_street_2 = wareRecord.w_street_2;
            paymentTxn.w_city = wareRecord.w_city;
            paymentTxn.w_state = wareRecord.w_state;
            paymentTxn.w_zip = wareRecord.w_zip;
            wareRecord.w_ytd += paymentTxn.h_amount;
            wareCache.put(wareRecord.getKey(), wareRecord);

            DistRecord distRecord = distCache.get(DistRecord.getKey(w_id, paymentTxn.d_id));
            String d_name = distRecord.d_name;
            paymentTxn.d_street_1 = distRecord.d_street_1;
            paymentTxn.d_street_2 = distRecord.d_street_2;
            paymentTxn.d_city = distRecord.d_city;
            paymentTxn.d_state = distRecord.d_state;
            paymentTxn.d_zip = distRecord.d_zip;
            distRecord.d_ytd += paymentTxn.h_amount;
            distCache.put(distRecord.getKey(), distRecord);

            CustRecord custRecord;
            if (paymentTxn.c_id > 0) {
                custRecord = (CustRecord)Query.findOne("CUSTOMER", custCache,
                        ImmutableMap.of("c_w_id", w_id, "c_d_id", paymentTxn.d_id, "c_id", paymentTxn.c_id));
                paymentTxn.c_last = custRecord.c_last;
            } else {
                List<CustRecord> custRecords = (List<CustRecord>)Query.find("CUSTOMER", custCache,
                        ImmutableMap.of("c_w_id", w_id, "c_d_id", paymentTxn.d_id), null,
                        ImmutableMap.of("c_last", paymentTxn.c_last), "c_first");
                custRecord = custRecords.get(custRecords.size()/2);
                paymentTxn.c_id = custRecord.c_id;
            }
            custRecord.c_balance -= paymentTxn.h_amount;
            custRecord.c_ytd_payment += paymentTxn.h_amount;
            custRecord.c_payment_cnt ++;
            if (custRecord.c_credit.equals("BC")) {
                custRecord.c_data = custRecord.getKey() + "&" + DistRecord.getKey(w_id, paymentTxn.d_id)
                        + "&H_AMOUNT" + paymentTxn.h_amount + custRecord.c_data;
                if (custRecord.c_data.length() > 500)
                    custRecord.c_data = custRecord.c_data.substring(0, 500);
            }
            custCache.put(custRecord.getKey(), custRecord);

            paymentTxn.c_first = custRecord.c_first;
            paymentTxn.c_middle = custRecord.c_middle;
            paymentTxn.c_street_1 = custRecord.c_street_1;
            paymentTxn.c_street_2 = custRecord.c_street_2;
            paymentTxn.c_city = custRecord.c_city;
            paymentTxn.c_state = custRecord.c_state;
            paymentTxn.c_zip = custRecord.c_zip;
            paymentTxn.c_phone = custRecord.c_phone;
            paymentTxn.c_since = custRecord.c_since;
            paymentTxn.c_credit = custRecord.c_credit;
            paymentTxn.c_credit_lim = custRecord.c_credit_lim;
            paymentTxn.c_discount = custRecord.c_discount;
            paymentTxn.c_balance = custRecord.c_balance;

            paymentTxn.h_date = new Date();
            HistRecord histRecord = new HistRecord(paymentTxn.c_id, paymentTxn.c_d_id, paymentTxn.c_w_id,
                    paymentTxn.d_id, w_id, paymentTxn.h_date, paymentTxn.h_amount, w_name + "    " + d_name);
            histCache.put(histRecord.getKey(), histRecord);

            tx.commit();
        }
        return 0;
    }

    public int doOrderStatus(OrderStatusTxn orderStatusTxn) {

        return 0;
    }

    public int doDelivery(DeliveryTxn deliveryTxn) {
        return 0;
    }

    public int doStockLevel(StockLevelTxn stockLevelTxn) {
        try (Transaction tx = this.ignite.transactions().txStart()) {
            DistRecord distRecord = distCache.get(DistRecord.getKey(stockLevelTxn.w_id, stockLevelTxn.d_id));
            int d_next_o_id = distRecord.d_next_o_id;



            tx.commit();
        }
        return 0;
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
