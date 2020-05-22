package com.maomingming.tpcc.driver;

public class DriverFactory {
    public static Driver getDriver(String driverType) {
        switch (driverType) {
            case "KEY_VALUE_DRIVER":
                return new KeyValueDriver();
            case "SQL_DRIVER":
                return new SQLDriver();
            default:
                throw new RuntimeException("Unexpected value: " + driverType);
        }
    }
}
