package com.maomingming.tpcc;

import java.util.Random;

public class RandomGenerator {
    public static String lastname(){
        return "BAR";
    }

    public static int makeNumber(int x, int y) {
        Random rand = new Random();
        return rand.nextInt(y-x+1) + x;
    }

    public static String makeAlphaString(int x, int y) {
        String alphaNum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        char[] alphaNumArray = alphaNum.toCharArray();
        int alphaNumLen = alphaNum.length();
        Random rand = new Random();
        int len = rand.nextInt(y-x+1) + x;
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < len; i++) {
            buf.append(alphaNumArray[rand.nextInt(alphaNumLen)]);
        }
        return buf.toString();
    }
}
