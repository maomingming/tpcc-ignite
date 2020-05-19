package com.maomingming.tpcc;

import com.google.common.collect.ImmutableMap;
import com.maomingming.tpcc.execute.Executor;
import com.maomingming.tpcc.execute.KeyValueExecutor;
import com.maomingming.tpcc.execute.SQLExecutor;
import com.maomingming.tpcc.record.*;
import com.maomingming.tpcc.txn.*;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;

public class Worker {
    int w_id;
    Executor executor;

    public Worker(String executorType, int w_id) throws Exception {
        this.w_id = w_id;
        executor = getExecutor(executorType);
    }

    public int doNewOrder(NewOrderTxn newOrderTxn) {
        executor.txStart();
        Warehouse warehouse = (Warehouse) executor.findOne("WAREHOUSE",
                Arrays.asList("w_tax"),
                ImmutableMap.<String, Object>builder().put("w_id", w_id).build());
        newOrderTxn.w_tax = warehouse.w_tax;

        District district = (District) executor.findOne("DISTRICT",
                Arrays.asList("d_tax", "d_next_o_id"),
                ImmutableMap.<String, Object>builder().put("d_w_id", w_id).put("d_id", newOrderTxn.d_id).build());
        newOrderTxn.d_tax = district.d_tax;
        newOrderTxn.o_id = district.d_next_o_id;
        district.d_next_o_id++;
        executor.update("DISTRICT", Arrays.asList("d_next_o_id"), district);

        Customer customer = (Customer) executor.findOne("CUSTOMER",
                Arrays.asList("c_discount", "c_last", "c_credit"),
                ImmutableMap.<String, Object>builder().put("c_w_id", w_id).put("c_d_id", newOrderTxn.d_id).put("c_id", newOrderTxn.c_id).build());
        newOrderTxn.c_discount = customer.c_discount;
        newOrderTxn.c_last = customer.c_last;
        newOrderTxn.c_credit = customer.c_credit;

        newOrderTxn.o_ol_cnt = newOrderTxn.inputRepeatingGroups.length;
        newOrderTxn.o_entry_d = new Date();

        boolean all_local = true;
        for (int i = 0; i < newOrderTxn.o_ol_cnt; i++) {
            if (newOrderTxn.inputRepeatingGroups[i].ol_supply_w_id != w_id) {
                all_local = false;
                break;
            }
        }

        Order order = new Order(newOrderTxn.o_id, newOrderTxn.d_id, w_id, newOrderTxn.c_id, newOrderTxn.o_entry_d, newOrderTxn.o_ol_cnt, all_local);
        executor.insert("ORDER", order);
        NewOrder newOrder = new NewOrder(newOrderTxn.o_id, newOrderTxn.d_id, w_id);
        executor.insert("NEW_ORDER", newOrder);

        for (int i = 0; i < newOrderTxn.o_ol_cnt; i++) {
            NewOrderTxn.InputRepeatingGroup input = newOrderTxn.inputRepeatingGroups[i];
            NewOrderTxn.OutputRepeatingGroup output = newOrderTxn.outputRepeatingGroups[i];

            Item item = (Item) executor.findOne("ITEM",
                    Arrays.asList("i_price", "i_name", "i_data"),
                    ImmutableMap.<String, Object>builder().put("i_id", input.ol_i_id).build());
            if (item == null) {
                executor.txRollback();
                return -1;
            }
            output.i_price = item.i_price;
            output.i_name = item.i_name;

            String dist = "s_dist_" + String.format("%02d", newOrderTxn.d_id);
            Stock stock = (Stock) executor.findOne("STOCK",
                    Arrays.asList("s_quantity", dist, "s_data", "s_ytd", "s_order_cnt", "s_remote_cnt"),
                    ImmutableMap.<String, Object>builder().put("s_w_id", input.ol_supply_w_id).put("s_i_id", input.ol_i_id).build());
            if (stock.s_quantity >= input.ol_quantity + 10)
                stock.s_quantity -= input.ol_quantity;
            else
                stock.s_quantity += 91 - input.ol_quantity;
            stock.s_ytd += input.ol_quantity;
            stock.s_order_cnt++;
            if (input.ol_supply_w_id != w_id)
                stock.s_remote_cnt++;
            executor.update("STOCK", Arrays.asList("s_quantity", "s_ytd", "s_order_cnt", "s_remote_cnt"), stock);
            output.s_quantity = stock.s_quantity;
            String s_dist = null;
            try {
                Field f = Stock.class.getField(dist);
                s_dist = (String) f.get(stock);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }

            output.ol_amount = item.i_price.multiply(new BigDecimal(input.ol_quantity));

            if (item.i_data.contains("ORIGINAL") && stock.s_data.contains("ORIGINAL"))
                output.brand_generic = 'B';
            else
                output.brand_generic = 'G';

            OrderLine orderLine = new OrderLine(newOrderTxn.o_id, newOrderTxn.d_id, w_id,
                    i + 1, input.ol_i_id, input.ol_supply_w_id, input.ol_quantity,
                    output.ol_amount, s_dist);
            executor.insert("ORDER_LINE", orderLine);

            newOrderTxn.totalAmount = newOrderTxn.totalAmount.add(output.ol_amount);
        }
        newOrderTxn.totalAmount = newOrderTxn.totalAmount.multiply(customer.c_discount.negate().add(BigDecimal.valueOf(1)))
                .multiply(warehouse.w_tax.add(district.d_tax).add(BigDecimal.valueOf(1)));
        executor.txCommit();
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

    public void finish() {
        executor.executeFinish();
    }

    private Executor getExecutor(String executorType) throws Exception {
        switch (executorType) {
            case "KEY_VALUE_EXECUTOR":
                return new KeyValueExecutor();
            case "SQL_EXECUTOR":
                return new SQLExecutor();
            default:
                throw new IllegalStateException("Unexpected value: " + executorType);
        }
    }
}
