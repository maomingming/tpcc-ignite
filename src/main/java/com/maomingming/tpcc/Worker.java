package com.maomingming.tpcc;

import com.google.common.collect.ImmutableMap;
import com.maomingming.tpcc.driver.Driver;
import com.maomingming.tpcc.driver.DriverFactory;
import com.maomingming.tpcc.driver.KeyValueDriver;
import com.maomingming.tpcc.driver.SQLDriver;
import com.maomingming.tpcc.param.Aggregation;
import com.maomingming.tpcc.param.Projection;
import com.maomingming.tpcc.param.Query;
import com.maomingming.tpcc.param.Update;
import com.maomingming.tpcc.record.*;
import com.maomingming.tpcc.txn.*;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Worker {
    int w_id;
    Driver driver;

    public Worker(String driverType, int w_id) throws Exception {
        this.w_id = w_id;
        driver = DriverFactory.getDriver(driverType);
        driver.runtimeStart();
    }

    public int doNewOrder(NewOrderTxn newOrderTxn) throws TransactionRetryException {
        driver.txStart();
        Warehouse warehouse = (Warehouse) driver.findOne("WAREHOUSE",
                new Query(ImmutableMap.of("w_id", w_id)),
                new Projection("Warehouse", Collections.singletonList("w_tax")));
        newOrderTxn.w_tax = warehouse.w_tax;

        Query districtQuery = new Query(ImmutableMap.of("d_w_id", w_id,"d_id", newOrderTxn.d_id));
        District district = (District) driver.findOne("DISTRICT",
                districtQuery, new Projection("District", Arrays.asList("d_tax", "d_next_o_id")));
        newOrderTxn.d_tax = district.d_tax;
        newOrderTxn.o_id = district.d_next_o_id;
        driver.update("DISTRICT", districtQuery,
                new Update(ImmutableMap.of("d_next_o_id", 1)));

        Customer customer = (Customer) driver.findOne("CUSTOMER",
                new Query(ImmutableMap.of("c_w_id", w_id, "c_d_id", newOrderTxn.d_id, "c_id", newOrderTxn.c_id)),
                new Projection("Customer", Arrays.asList("c_discount", "c_last", "c_credit")));
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
        driver.insert("ORDER", order);
        NewOrder newOrder = new NewOrder(newOrderTxn.o_id, newOrderTxn.d_id, w_id);
        driver.insert("NEW_ORDER", newOrder);

        newOrderTxn.totalAmount = BigDecimal.valueOf(0);
        for (int i = 0; i < newOrderTxn.o_ol_cnt; i++) {
            NewOrderTxn.InputRepeatingGroup input = newOrderTxn.inputRepeatingGroups[i];
            NewOrderTxn.OutputRepeatingGroup output = newOrderTxn.outputRepeatingGroups[i];

            Item item = (Item) driver.findOne("ITEM",
                    new Query(ImmutableMap.of("i_id", input.ol_i_id)),
                    new Projection("Item", Arrays.asList("i_price", "i_name", "i_data")));
            if (item == null) {
                driver.txRollback();
                newOrderTxn.isRollback=true;
                return 0;
            }
            output.i_price = item.i_price;
            output.i_name = item.i_name;

            String dist = "s_dist_" + String.format("%02d", newOrderTxn.d_id);
            Query stockQuery = new Query(ImmutableMap.of("s_w_id", input.ol_supply_w_id, "s_i_id", input.ol_i_id));
            Stock stock = (Stock) driver.findOne("STOCK",
                    stockQuery, new Projection("Stock", Arrays.asList("s_quantity", dist, "s_data")));
            ImmutableMap.Builder<String, Integer> m = ImmutableMap.builder();
            m.put("s_ytd", input.ol_quantity);
            m.put("s_order_cnt", 1);
            if (input.ol_supply_w_id != w_id)
                m.put("s_remote_cnt", 1);
            driver.update("STOCK", stockQuery,
                    new Update(m.build(), null, ImmutableMap.of("s_quantity", output.s_quantity)));
            String s_dist = null;
            try {
                Field f = Stock.class.getField(dist);
                s_dist = (String) f.get(stock);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }
            if (stock.s_quantity >= input.ol_quantity + 10)
                output.s_quantity = stock.s_quantity - input.ol_quantity;
            else
                output.s_quantity = stock.s_quantity - input.ol_quantity + 91;

            output.ol_amount = item.i_price.multiply(new BigDecimal(input.ol_quantity));

            if (item.i_data.contains("ORIGINAL") && stock.s_data.contains("ORIGINAL"))
                output.brand_generic = 'B';
            else
                output.brand_generic = 'G';

            OrderLine orderLine = new OrderLine(newOrderTxn.o_id, newOrderTxn.d_id, w_id,
                    i + 1, input.ol_i_id, input.ol_supply_w_id, input.ol_quantity,
                    output.ol_amount, s_dist);
            driver.insert("ORDER_LINE", orderLine);

            newOrderTxn.totalAmount = newOrderTxn.totalAmount.add(output.ol_amount);
        }
        newOrderTxn.totalAmount = newOrderTxn.totalAmount.multiply(customer.c_discount.negate().add(BigDecimal.valueOf(1)))
                .multiply(warehouse.w_tax.add(district.d_tax).add(BigDecimal.valueOf(1)));
        driver.txCommit();
        return 0;
    }

    public int doPayment(PaymentTxn paymentTxn) throws TransactionRetryException {
        driver.txStart();
        Query wareQuery = new Query(ImmutableMap.of("w_id", w_id));
        Warehouse warehouse = (Warehouse) driver.findOne("WAREHOUSE", wareQuery,
                new Projection("Warehouse", Arrays.asList("w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip")));
        String w_name = warehouse.w_name;
        paymentTxn.w_street_1 = warehouse.w_street_1;
        paymentTxn.w_street_2 = warehouse.w_street_2;
        paymentTxn.w_city = warehouse.w_city;
        paymentTxn.w_state = warehouse.w_state;
        paymentTxn.w_zip = warehouse.w_zip;
        driver.update("WAREHOUSE", wareQuery,
                new Update(null, ImmutableMap.of("w_ytd", paymentTxn.h_amount)));

        Query distQuery = new Query(ImmutableMap.of("d_w_id", w_id, "d_id", paymentTxn.d_id));
        District district = (District) driver.findOne("DISTRICT", distQuery,
                new Projection("District", Arrays.asList("d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip")));
        String d_name = district.d_name;
        paymentTxn.d_street_1 = district.d_street_1;
        paymentTxn.d_street_2 = district.d_street_2;
        paymentTxn.d_city = district.d_city;
        paymentTxn.d_state = district.d_state;
        paymentTxn.d_zip = district.d_zip;
        driver.update("DISTRICT", distQuery,
                new Update(null, ImmutableMap.of("d_ytd", paymentTxn.h_amount)));

        Customer customer;
        if (paymentTxn.c_id > 0) {
            customer = (Customer) driver.findOne("CUSTOMER",
                    new Query(ImmutableMap.of("c_w_id", w_id, "c_d_id", paymentTxn.d_id, "c_id", paymentTxn.c_id)),
                    new Projection("Customer", Arrays.asList("c_last", "c_credit", "c_data",
                            "c_first", "c_middle", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip",
                            "c_phone", "c_since", "c_credit_lim", "c_discount", "c_balance")));
            paymentTxn.c_last = customer.c_last;
        } else {
            customer = (Customer) driver.findOne("CUSTOMER",
                    new Query(ImmutableMap.of("c_w_id", w_id, "c_d_id", paymentTxn.d_id, "c_last", paymentTxn.c_last)),
                    new Projection("Customer", Arrays.asList("c_id", "c_credit", "c_data",
                            "c_first", "c_middle", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip",
                            "c_phone", "c_since", "c_credit_lim", "c_discount", "c_balance"), "c_first", "ASC", "MID"));
            if (customer == null) {
                driver.txRollback();
                return -1;
            }
            paymentTxn.c_id = customer.c_id;
        }
        Map<String, Object> replace = null;
        if (customer.c_credit.equals("BC")) {
            paymentTxn.c_data = Customer.getKey(w_id, paymentTxn.d_id, paymentTxn.c_id) + "&" + District.getKey(w_id, paymentTxn.d_id)
                    + "&H_AMOUNT" + paymentTxn.h_amount + customer.c_data;
            if (paymentTxn.c_data.length() > 500)
                paymentTxn.c_data = paymentTxn.c_data.substring(0, 500);
            replace = ImmutableMap.of("c_data", paymentTxn.c_data);
        }
        driver.update("CUSTOMER",
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
        paymentTxn.c_balance = customer.c_balance.subtract(paymentTxn.h_amount);

        paymentTxn.h_date = new Date();
        History history = new History(paymentTxn.c_id, paymentTxn.c_d_id, paymentTxn.c_w_id,
                paymentTxn.d_id, w_id, paymentTxn.h_date, paymentTxn.h_amount, w_name + "    " + d_name);
        driver.insert("HISTORY", history);

        driver.txCommit();
        return 0;
    }

    public int doOrderStatus(OrderStatusTxn orderStatusTxn) {
        driver.txStart();
        Customer customer;
        if (orderStatusTxn.c_id > 0) {
            customer = (Customer) driver.findOne("CUSTOMER",
                    new Query(ImmutableMap.of("c_w_id", w_id, "c_d_id", orderStatusTxn.d_id, "c_id", orderStatusTxn.c_id)),
                    new Projection("Customer", Arrays.asList("c_last", "c_balance", "c_first", "c_middle")));
            orderStatusTxn.c_last = customer.c_last;
        } else {
            customer = (Customer) driver.findOne("CUSTOMER",
                    new Query(ImmutableMap.of("c_w_id", w_id, "c_d_id", orderStatusTxn.d_id, "c_last", orderStatusTxn.c_last)),
                    new Projection("Customer", Arrays.asList("c_id", "c_balance", "c_first", "c_middle"),
                            "c_first", "ASC", "MID"));
            if (customer == null) {
                driver.txRollback();
                return -1;
            }
            orderStatusTxn.c_id = customer.c_id;
        }

        orderStatusTxn.c_balance = customer.c_balance;
        orderStatusTxn.c_first = customer.c_first;
        orderStatusTxn.c_middle = customer.c_middle;

        Order order = (Order) driver.findOne("ORDER",
                new Query(ImmutableMap.of("o_w_id", w_id, "o_d_id", orderStatusTxn.d_id, "o_c_id", orderStatusTxn.c_id)),
                new Projection("Order", Arrays.asList("o_id", "o_entry_d", "o_carrier_id", "o_ol_cnt"), "o_id", "DESC", "FIRST"));
        orderStatusTxn.o_id = order.o_id;
        orderStatusTxn.o_entry_d = order.o_entry_d;
        orderStatusTxn.o_carrier_id = order.o_carrier_id;

        orderStatusTxn.outputRepeatingGroups = new OrderStatusTxn.OutputRepeatingGroup[order.o_ol_cnt];
        for (int i = 1; i <= order.o_ol_cnt; i++) {
            OrderLine orderLine = (OrderLine) driver.find("ORDER_LINE",
                    new Query(ImmutableMap.of("ol_w_id", w_id, "ol_d_id", orderStatusTxn.d_id, "ol_o_id", orderStatusTxn.o_id, "ol_number", i)),
                    new Projection("OrderLine", Arrays.asList("ol_i_id", "ol_supply_w_id", "ol_quantity", "ol_amount", "ol_delivery_d")));
            orderStatusTxn.outputRepeatingGroups[i] = new OrderStatusTxn.OutputRepeatingGroup(
                    orderLine.ol_i_id, orderLine.ol_supply_w_id, orderLine.ol_quantity, orderLine.ol_amount, orderLine.ol_delivery_d
            );
        }
        driver.txCommit();
        return 0;
    }

    public int doDelivery(DeliveryTxn deliveryTxn) throws TransactionRetryException {
        for (int d_id = deliveryTxn.start_d; d_id <= 10; ++d_id) {
            driver.txStart();
            NewOrder newOrder = (NewOrder) driver.findOne("NEW_ORDER",
                    new Query(ImmutableMap.of("no_w_id", w_id, "no_d_id", d_id)),
                    new Projection("NewOrder", Collections.singletonList("no_o_id"), "no_o_id", "DESC", "FIRST"));
            if (newOrder == null)
                continue;
            int o_id = newOrder.no_o_id;
            driver.delete("NEW_ORDER", new Query(ImmutableMap.of("no_w_id", w_id, "no_d_id", d_id, "no_o_id", o_id)));

            Query orderQuery = new Query(ImmutableMap.of("o_w_id", w_id, "o_d_id", d_id, "o_id", o_id));
            Order order = (Order) driver.findOne("ORDER", orderQuery,
                    new Projection("Order", Collections.singletonList("o_c_id")));
            driver.update("ORDER", orderQuery, new Update(null, null, ImmutableMap.of("o_carrier_id", deliveryTxn.o_carrier_id)));
            int c_id = order.o_c_id;

            Query orderLineQuery = new Query(ImmutableMap.of("ol_w_id", w_id, "ol_d_id", d_id, "ol_o_id", o_id));
            BigDecimal amount = (BigDecimal) driver.aggregation("ORDER_LINE", orderLineQuery, new Aggregation("SUM", "ol_amount", "DECIMAL"));
            driver.update("ORDER_LINE", orderLineQuery,
                    new Update(null, null, ImmutableMap.of("ol_delivery_d", new Date())));

            driver.update("CUSTOMER",
                    new Query(ImmutableMap.of("c_w_id", w_id, "c_d_id", d_id, "c_id", c_id)),
                    new Update(ImmutableMap.of("c_delivery_cnt", 1), ImmutableMap.of("c_balance", amount)));
            driver.txCommit();
            deliveryTxn.start_d++;
        }
        return 0;
    }

    public int doStockLevel(StockLevelTxn stockLevelTxn) {
//        driver.txStart();
        District district = (District) driver.findOne("DISTRICT",
                new Query(ImmutableMap.of("d_w_id", w_id, "d_id", stockLevelTxn.d_id)),
                new Projection("District", Collections.singletonList("d_next_o_id")));
        int next_o_id = district.d_next_o_id;

        List<Record> orderLines = driver.find("ORDER_LINE",
                new Query(ImmutableMap.of("ol_w_id", w_id, "ol_d_id", stockLevelTxn.d_id),
                        ImmutableMap.of("ol_o_id", IntStream.range(next_o_id-20, next_o_id).boxed().collect(Collectors.toSet()))),
                new Projection("OrderLine", Collections.singletonList("ol_i_id")));

        Set<Integer> i_ids = new HashSet<>();
        for (Record orderLine : orderLines) {
            i_ids.add(((OrderLine) orderLine).ol_i_id);
        }
        stockLevelTxn.low_stock = (long) driver.aggregation("STOCK",
                new Query(ImmutableMap.of("s_w_id", w_id),
                        ImmutableMap.of("s_i_id", i_ids),
                        ImmutableMap.of("s_quantity", stockLevelTxn.threshold)),
                new Aggregation("COUNT"));
//        driver.txCommit();
        return 0;
    }

    public void rollback() {
        driver.txRollback();
    }

    public void finish() {
        driver.runtimeFinish();
    }

}
