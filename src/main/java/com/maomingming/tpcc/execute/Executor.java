package com.maomingming.tpcc.execute;

import com.maomingming.tpcc.txn.*;

public interface Executor {
    int doNewOrder(NewOrderTxn newOrderTxn);
    int doPayment(PaymentTxn paymentTxn);
    int doOrderStatus(OrderStatusTxn orderStatusTxn);
    int doDelivery(DeliveryTxn deliveryTxn);
    int doStockLevel(StockLevelTxn stockLevelTxn);
    void executeFinish();
}
