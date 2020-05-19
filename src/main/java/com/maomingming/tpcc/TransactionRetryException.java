package com.maomingming.tpcc;

public class TransactionRetryException extends Exception{
    public TransactionRetryException() {
        super();
    }
    public TransactionRetryException(String message) {
        super(message);
    }
}
