package com.maomingming.tpcc;

public class Coordinator {

    static int w_cnt = 3;
    static String loaderType = "STREAM_LOADER";
    static String executorType = "KEY_VALUE_EXECUTOR";

    public static void main(String[] args) throws Exception {
        Populator populator = new Populator(loaderType, w_cnt);
        populator.loadAll();

        new Counter().start();
        for (int w_id = 1; w_id <= w_cnt; w_id ++) {
            for (int t_id = 1; t_id <= 10; t_id ++) {
                new Emulator(executorType, w_id, t_id, w_cnt).start();
            }
        }
    }
}
