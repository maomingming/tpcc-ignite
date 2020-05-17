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
    IgniteCache<String, Customer> custCache;
    IgniteCache<String, District> distCache;
    IgniteCache<String, History> histCache;
    IgniteCache<String, Item> itemCache;
    IgniteCache<String, NewOrder> newOrdCache;
    IgniteCache<String, OrderLine> ordLineCache;
    IgniteCache<String, Order> ordCache;
    IgniteCache<String, Stock> stockCache;
    IgniteCache<String, Warehouse> wareCache;

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
//        try (Transaction tx = this.ignite.transactions().txStart()) {
//            Warehouse warehouse = this.wareCache.get(Warehouse.getKey(w_id));
//            newOrderTxn.w_tax = warehouse.w_tax;
//
//            District district = this.distCache.get(District.getKey(w_id, newOrderTxn.d_id));
//            newOrderTxn.d_tax = district.d_tax;
//            newOrderTxn.o_id = district.d_next_o_id;
//            district.d_next_o_id ++;
//            this.distCache.put(district.getKey(), district);
//
//            Customer customer = this.custCache.get(Customer.getKey(w_id, newOrderTxn.d_id, newOrderTxn.c_id));
////            newOrderTxn.c_discount = customer.c_discount;
//            newOrderTxn.c_last = customer.c_last;
//            newOrderTxn.c_credit = customer.c_credit;
//
//            newOrderTxn.o_ol_cnt = newOrderTxn.inputRepeatingGroups.length;
//
//            boolean all_local = true;
//            for (int i = 0; i < newOrderTxn.o_ol_cnt; i++) {
//                if (newOrderTxn.inputRepeatingGroups[i].ol_supply_w_id != w_id) {
//                    all_local = false;
//                    break;
//                }
//            }
//
//            Order order = new Order(newOrderTxn.o_id, newOrderTxn.c_id, newOrderTxn.d_id, w_id, newOrderTxn.o_ol_cnt, all_local);
//            this.ordCache.put(order.getKey(), order);
//            NewOrder newOrder =  new NewOrder(newOrderTxn.o_id, newOrderTxn.d_id, w_id);
//            this.newOrdCache.put(newOrder.getKey(), newOrder);
//
//            for (int i = 0; i < newOrderTxn.o_ol_cnt; i++) {
//                NewOrderTxn.InputRepeatingGroup input = newOrderTxn.inputRepeatingGroups[i];
//                NewOrderTxn.OutputRepeatingGroup output = newOrderTxn.outputRepeatingGroups[i];
//
//                Item item = this.itemCache.get(Item.getKey(input.ol_i_id));
//                if (item == null) {
//                    tx.rollback();
//                    return -1;
//                }
//                output.i_price = item.i_price;
//                output.i_name = item.i_name;
//
//                Stock stock = this.stockCache.get(Stock.getKey(input.ol_supply_w_id, input.ol_i_id));
//                if (stock.s_quantity >= input.ol_quantity + 10)
//                    stock.s_quantity -= input.ol_quantity;
//                else
//                    stock.s_quantity += 91 - input.ol_quantity;
//                stock.s_ytd += input.ol_quantity;
//                stock.s_order_cnt ++;
//                if (input.ol_supply_w_id != w_id)
//                    stock.s_remote_cnt ++;
//                stockCache.put(stock.getKey(), stock);
//                output.s_quantity = stock.s_quantity;
//
//                output.ol_amount = input.ol_quantity * item.i_price;
//
//                if (item.i_data.contains("ORIGINAL") && stock.s_data.contains("ORIGINAL"))
//                    output.brand_generic = 'B';
//                else
//                    output.brand_generic = 'G';
//
//                OrderLine orderLine = new OrderLine(newOrderTxn.o_id, newOrderTxn.d_id, w_id,
//                        i + 1, input.ol_i_id, input.ol_supply_w_id, input.ol_quantity,
//                        output.ol_amount, stock.getDistInfo(newOrderTxn.d_id));
//                ordLineCache.put(orderLine.getKey(), orderLine);
//
//                newOrderTxn.totalAmount += output.ol_amount;
//            }
////            newOrderTxn.totalAmount *= (1 - customer.c_discount) * (1 + warehouse.w_tax + district.d_tax);
//            newOrderTxn.o_entry_d = new Date();
//            tx.commit();
//        }
        return 0;
    }

    @SuppressWarnings("unchecked")
    public int doPayment(PaymentTxn paymentTxn) {
//        try (Transaction tx = this.ignite.transactions().txStart()) {
//            Warehouse warehouse = wareCache.get(Warehouse.getKey(w_id));
//            String w_name = warehouse.w_name;
//            paymentTxn.w_street_1 = warehouse.w_street_1;
//            paymentTxn.w_street_2 = warehouse.w_street_2;
//            paymentTxn.w_city = warehouse.w_city;
//            paymentTxn.w_state = warehouse.w_state;
//            paymentTxn.w_zip = warehouse.w_zip;
//            warehouse.w_ytd += paymentTxn.h_amount;
//            wareCache.put(warehouse.getKey(), warehouse);
//
//            District district = distCache.get(District.getKey(w_id, paymentTxn.d_id));
//            String d_name = district.d_name;
//            paymentTxn.d_street_1 = district.d_street_1;
//            paymentTxn.d_street_2 = district.d_street_2;
//            paymentTxn.d_city = district.d_city;
//            paymentTxn.d_state = district.d_state;
//            paymentTxn.d_zip = district.d_zip;
//            district.d_ytd += paymentTxn.h_amount;
//            distCache.put(district.getKey(), district);
//
//            Customer customer;
//            if (paymentTxn.c_id > 0) {
//                customer = (Customer)Query.findOne("CUSTOMER", custCache,
//                        ImmutableMap.of("c_w_id", w_id, "c_d_id", paymentTxn.d_id, "c_id", paymentTxn.c_id));
//                paymentTxn.c_last = customer.c_last;
//            } else {
//                List<Customer> custRecords = (List<Customer>)Query.find("CUSTOMER", custCache,
//                        ImmutableMap.of("c_w_id", w_id, "c_d_id", paymentTxn.d_id), null,
//                        ImmutableMap.of("c_last", paymentTxn.c_last), "c_first");
//                customer = custRecords.get(custRecords.size()/2);
//                paymentTxn.c_id = customer.c_id;
//            }
////            customer.c_balance -= paymentTxn.h_amount;
////            customer.c_ytd_payment += paymentTxn.h_amount;
//            customer.c_payment_cnt ++;
//            if (customer.c_credit.equals("BC")) {
//                customer.c_data = customer.getKey() + "&" + District.getKey(w_id, paymentTxn.d_id)
//                        + "&H_AMOUNT" + paymentTxn.h_amount + customer.c_data;
//                if (customer.c_data.length() > 500)
//                    customer.c_data = customer.c_data.substring(0, 500);
//            }
//            custCache.put(customer.getKey(), customer);
//
//            paymentTxn.c_first = customer.c_first;
//            paymentTxn.c_middle = customer.c_middle;
//            paymentTxn.c_street_1 = customer.c_street_1;
//            paymentTxn.c_street_2 = customer.c_street_2;
//            paymentTxn.c_city = customer.c_city;
//            paymentTxn.c_state = customer.c_state;
//            paymentTxn.c_zip = customer.c_zip;
//            paymentTxn.c_phone = customer.c_phone;
//            paymentTxn.c_since = customer.c_since;
//            paymentTxn.c_credit = customer.c_credit;
////            paymentTxn.c_credit_lim = customer.c_credit_lim;
////            paymentTxn.c_discount = customer.c_discount;
////            paymentTxn.c_balance = customer.c_balance;
//
//            paymentTxn.h_date = new Date();
//            History history = new History(paymentTxn.c_id, paymentTxn.c_d_id, paymentTxn.c_w_id,
//                    paymentTxn.d_id, w_id, paymentTxn.h_date, paymentTxn.h_amount, w_name + "    " + d_name);
//            histCache.put(history.getKey(), history);
//
//            tx.commit();
//        }
        return 0;
    }

    public int doOrderStatus(OrderStatusTxn orderStatusTxn) {

        return 0;
    }

    public int doDelivery(DeliveryTxn deliveryTxn) {
        return 0;
    }

    public int doStockLevel(StockLevelTxn stockLevelTxn) {
//        try (Transaction tx = this.ignite.transactions().txStart()) {
//            District district = distCache.get(District.getKey(stockLevelTxn.w_id, stockLevelTxn.d_id));
//            int d_next_o_id = district.d_next_o_id;
//
//
//
//            tx.commit();
//        }
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
