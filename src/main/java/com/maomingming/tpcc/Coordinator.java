package com.maomingming.tpcc;

public class Coordinator {

    static int w_cnt = 3;
    static String driverType = "SQL_DRIVER";

    public static void main(String[] args) throws Exception {
        Populator populator = new Populator(driverType, w_cnt);
        populator.loadAll();

        new Counter().start();
        for (int w_id = 1; w_id <= w_cnt; w_id ++) {
            for (int t_id = 1; t_id <= 10; t_id ++) {
                new Emulator(driverType, w_id, t_id, w_cnt).start();
            }
        }
    }
}
