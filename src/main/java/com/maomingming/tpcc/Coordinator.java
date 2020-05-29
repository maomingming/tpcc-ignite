package com.maomingming.tpcc;

import com.maomingming.tpcc.driver.SQLDriver;

import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Coordinator {

    static int w_cnt = 45;
    static String driverType = "SQL_DRIVER";

    public static void main(String[] args) throws Exception {
        System.out.println("start now");
        File dir = new File("result");
        dir.mkdirs();

        long begin = System.currentTimeMillis();
        List<Populator> populators = new ArrayList<>();
        if (driverType.equals("SQL_DRIVER")) {
            SQLDriver.createSchema();
            for (int w_id = 1; w_id <= w_cnt; w_id++) {
                Populator populator = new Populator(driverType, Collections.singletonList(w_id), w_id == 1);
                populator.start();
                populators.add(populator);
            }
        } else if (driverType.equals("KEY_VALUE_DRIVER")) {
            Populator populator = new Populator(driverType, IntStream.range(1, w_cnt + 1).boxed().collect(Collectors.toList()), true);
            populator.start();
            populators.add(populator);
        }
        for (Populator populator : populators) {
            populator.join();
        }
        long end = System.currentTimeMillis();
        System.out.printf("population done: %dms\n", end - begin);

        new Counter().start();
        for (int w_id = 1; w_id <= w_cnt; w_id++) {
            for (int t_id = 1; t_id <= 10; t_id++) {
                new Emulator(driverType, w_id, t_id, w_cnt).start();
            }
        }
    }
}
