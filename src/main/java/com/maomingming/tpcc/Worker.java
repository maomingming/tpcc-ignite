package com.maomingming.tpcc;

import com.google.common.collect.ImmutableMap;
import com.maomingming.tpcc.execute.Executor;
import com.maomingming.tpcc.execute.KeyValueExecutor;
import com.maomingming.tpcc.execute.SQLExecutor;
import com.maomingming.tpcc.param.Projection;
import com.maomingming.tpcc.param.Query;
import com.maomingming.tpcc.param.Update;
import com.maomingming.tpcc.record.*;
import com.maomingming.tpcc.txn.*;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class Worker {
    int w_id;
    KeyValueExecutor executor;

    public Worker(String executorType, int w_id) throws Exception {
        this.w_id = w_id;
        executor = new KeyValueExecutor();
//        executor = getExecutor(executorType);
    }

    public int doNewOrder(NewOrderTxn newOrderTxn) throws TransactionRetryException {
        executor.txStart();
        Warehouse warehouse = (Warehouse) executor.findOne("WAREHOUSE",
                new Query(ImmutableMap.<String, Object>builder().put("w_id", w_id).build()),
                new Projection(Arrays.asList("w_tax")));
        newOrderTxn.w_tax = warehouse.w_tax;

        Query districtQuery = new Query(ImmutableMap.<String, Object>builder().put("d_w_id", w_id).put("d_id", newOrderTxn.d_id).build());
        District district = (District) executor.findOne("DISTRICT",
                districtQuery, new Projection(Arrays.asList("d_tax", "d_next_o_id")));
        newOrderTxn.d_tax = district.d_tax;
        newOrderTxn.o_id = district.d_next_o_id;
        executor.update("DISTRICT", districtQuery,
                new Update(ImmutableMap.<String, Integer>builder().put("d_next_o_id", 1).build()));

        Customer customer = (Customer) executor.findOne("CUSTOMER",
                new Query(ImmutableMap.<String, Object>builder().put("c_w_id", w_id).put("c_d_id", newOrderTxn.d_id).put("c_id", newOrderTxn.c_id).build()),
                new Projection(Arrays.asList("c_discount", "c_last", "c_credit")));
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

        newOrderTxn.totalAmount = BigDecimal.valueOf(0);
        for (int i = 0; i < newOrderTxn.o_ol_cnt; i++) {
            NewOrderTxn.InputRepeatingGroup input = newOrderTxn.inputRepeatingGroups[i];
            NewOrderTxn.OutputRepeatingGroup output = newOrderTxn.outputRepeatingGroups[i];

            Item item = (Item) executor.findOne("ITEM",
                    new Query(ImmutableMap.of("i_id", input.ol_i_id)),
                    new Projection(Arrays.asList("i_price", "i_name", "i_data")));
            if (item == null) {
                executor.txRollback();
                return -1;
            }
            output.i_price = item.i_price;
            output.i_name = item.i_name;

            String dist = "s_dist_" + String.format("%02d", newOrderTxn.d_id);
            Query stockQuery = new Query(ImmutableMap.of("s_w_id", input.ol_supply_w_id, "s_i_id", input.ol_i_id));
            Stock stock = (Stock) executor.findOne("STOCK",
                    stockQuery, new Projection(Arrays.asList("s_quantity", dist, "s_data")));
            output.s_quantity = stock.s_quantity;
            String s_dist = null;
            try {
                Field f = Stock.class.getField(dist);
                s_dist = (String) f.get(stock);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }
            ImmutableMap.Builder<String, Integer> m = ImmutableMap.builder();
            if (stock.s_quantity >= input.ol_quantity + 10)
                m.put("s_quantity", -input.ol_quantity);
            else
                m.put("s_quantity", 91 - input.ol_quantity);
            m.put("s_ytd", input.ol_quantity);
            m.put("s_order_cnt", 1);
            if (input.ol_supply_w_id != w_id)
                m.put("s_remote_cnt", 1);
            executor.update("STOCK", stockQuery, new Update(m.build()));

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

    public int doPayment(PaymentTxn paymentTxn) throws TransactionRetryException {
        executor.txStart();
        Query wareQuery = new Query(ImmutableMap.of("w_id", w_id));
        Warehouse warehouse = (Warehouse) executor.findOne("WAREHOUSE", wareQuery,
                new Projection(Arrays.asList("w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip")));
        String w_name = warehouse.w_name;
        paymentTxn.w_street_1 = warehouse.w_street_1;
        paymentTxn.w_street_2 = warehouse.w_street_2;
        paymentTxn.w_city = warehouse.w_city;
        paymentTxn.w_state = warehouse.w_state;
        paymentTxn.w_zip = warehouse.w_zip;
        executor.update("WAREHOUSE", wareQuery,
                new Update(null, ImmutableMap.of("w_ytd", paymentTxn.h_amount)));

        Query distQuery = new Query(ImmutableMap.of("d_w_id", w_id, "d_id", paymentTxn.d_id));
        District district = (District) executor.findOne("DISTRICT", distQuery,
                new Projection(Arrays.asList("d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip")));
        String d_name = district.d_name;
        paymentTxn.d_street_1 = district.d_street_1;
        paymentTxn.d_street_2 = district.d_street_2;
        paymentTxn.d_city = district.d_city;
        paymentTxn.d_state = district.d_state;
        paymentTxn.d_zip = district.d_zip;
        executor.update("DISTRICT", distQuery,
                new Update(null, ImmutableMap.of("d_ytd", paymentTxn.h_amount)));

        Customer customer;
        if (paymentTxn.c_id > 0) {
            customer = (Customer) executor.findOne("CUSTOMER",
                    new Query(ImmutableMap.of("c_w_id", w_id, "c_d_id", paymentTxn.d_id, "c_id", paymentTxn.c_id)),
                    new Projection(Arrays.asList("c_last", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_credit", "c_data",
                            "c_first", "c_middle", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip",
                            "c_phone", "c_since", "c_credit_lim", "c_discount")));
            paymentTxn.c_last = customer.c_last;
        } else {
            customer = (Customer) executor.findOne("CUSTOMER",
                    new Query(ImmutableMap.of("c_w_id", w_id, "c_d_id", paymentTxn.d_id, "c_last", paymentTxn.c_last)),
                    new Projection(Arrays.asList("c_id", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_credit", "c_data",
                            "c_first", "c_middle", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip",
                            "c_phone", "c_since", "c_credit_lim", "c_discount"), "c_first", "ASC", "MID"));
            if (customer == null){
                executor.txRollback();
                return -1;
            }
            paymentTxn.c_id = customer.c_id;
        }
        customer.c_balance = customer.c_balance.subtract(paymentTxn.h_amount);
        customer.c_ytd_payment = customer.c_ytd_payment.add(paymentTxn.h_amount);
        customer.c_payment_cnt++;
        Map<String, Object> replace = null;
        if (customer.c_credit.equals("BC")) {
            String c_data = Customer.getKey(w_id, paymentTxn.d_id, paymentTxn.c_id) + "&" + District.getKey(w_id, paymentTxn.d_id)
                    + "&H_AMOUNT" + paymentTxn.h_amount + customer.c_data;
            if (c_data.length() > 500)
                c_data = c_data.substring(0, 500);
            replace = ImmutableMap.of("c_data", c_data);
        }
        executor.update("CUSTOMER",
                new Query(ImmutableMap.of("c_w_id", w_id, "c_d_id", paymentTxn.d_id, "c_id", paymentTxn.c_id)),
                new Update(ImmutableMap.of("c_payment_cnt", 1),
                        ImmutableMap.of("c_balance", paymentTxn.h_amount.negate(), "c_ytd_payment", paymentTxn.h_amount), replace));

        paymentTxn.c_first = customer.c_first;
        paymentTxn.c_middle = customer.c_middle;
        paymentTxn.c_street_1 = customer.c_street_1;
        paymentTxn.c_street_2 = customer.c_street_2;
        paymentTxn.c_city = customer.c_city;
        paymentTxn.c_state = customer.c_state;
        paymentTxn.c_zip = customer.c_zip;
        paymentTxn.c_phone = customer.c_phone;
        paymentTxn.c_since = customer.c_since;
        paymentTxn.c_credit = customer.c_credit;
        paymentTxn.c_credit_lim = customer.c_credit_lim;
        paymentTxn.c_discount = customer.c_discount;
        paymentTxn.c_balance = customer.c_balance;

        paymentTxn.h_date = new Date();
        History history = new History(paymentTxn.c_id, paymentTxn.c_d_id, paymentTxn.c_w_id,
                paymentTxn.d_id, w_id, paymentTxn.h_date, paymentTxn.h_amount, w_name + "    " + d_name);
        executor.insert("HISTORY", history);

        executor.txCommit();
        return 0;
    }
//
//    public int doOrderStatus(OrderStatusTxn orderStatusTxn) {
//        executor.txCommit();
//        Customer customer;
//        if (orderStatusTxn.c_id > 0) {
//            customer = (Customer)executor.findOne("CUSTOMER",
//                    Arrays.asList("c_last", "c_balance", "c_first", "c_middle"),
//                    ImmutableMap.of("c_w_id", w_id, "c_d_id", orderStatusTxn.d_id, "c_id", orderStatusTxn.c_id));
//            orderStatusTxn.c_last = customer.c_last;
//        } else {
//            List<Record> custRecords = executor.find("CUSTOMER",
//                    Arrays.asList("c_balance", "c_first", "c_middle"),
//                    ImmutableMap.of("c_w_id", w_id, "c_d_id", orderStatusTxn.d_id), null,
//                    ImmutableMap.of("c_last", orderStatusTxn.c_last), "c_first");
//            customer = (Customer) custRecords.get(custRecords.size()/2);
//        }
//        orderStatusTxn.c_balance = customer.c_balance;
//        orderStatusTxn.c_first = customer.c_first;
//        orderStatusTxn.c_middle = customer.c_middle;
//
//
//        return 0;
//    }

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

//    private Executor getExecutor(String executorType) throws Exception {
//        switch (executorType) {
//            case "KEY_VALUE_EXECUTOR":
//                return new KeyValueExecutor();
//            case "SQL_EXECUTOR":
//                return new SQLExecutor();
//            default:
//                throw new IllegalStateException("Unexpected value: " + executorType);
//        }
//    }
}
