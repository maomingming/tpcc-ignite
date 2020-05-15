package com.maomingming.tpcc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class RandomGenerator {

    static Random rand = new Random();

    static int c_last_load = makeNumber(0, 255);
    static int c_last_run;
    static {
        int delta;
        do {
            c_last_run = makeNumber(0, 255);
            delta = Math.abs(c_last_load - c_last_run);
        } while (delta < 65 || delta > 119 || delta == 96 || delta == 112);
    }
    static int c_id = makeNumber(0, 1023);
    static int ol_i_id = makeNumber(0, 8191);

    public static boolean makeBool(float p) {
        return rand.nextFloat() < p;
    }

    public static String makeLastName(int n) {
        if (n < 0 || n >= 1000) throw new AssertionError();
        String[] SYLLABLE = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
        return SYLLABLE[n / 100] + SYLLABLE[(n % 100) / 10] + SYLLABLE[n % 10];
    }

    public static String makeLastNameForLoad(int c_id) {
        if (c_id < 1000)
            return makeLastName(c_id);
        return makeLastName(makeNURand(255, 0, 999, c_last_load));
    }

    public static String makeLastNameForRun() {
        return makeLastName(makeNURand(255, 0, 999, c_last_run));
    }

    public static int makeNURand(int A, int x, int y) {
        int C;
        switch (A) {
            case 1023:
                C = c_id;
                break;
            case 8191:
                C = ol_i_id;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + A);
        }
        return makeNURand(A, x, y, C);
    }

    public static int makeNURand(int A, int x, int y, int C) {
        return (((makeNumber(0, A) | makeNumber(x, y)) + C) % (y - x + 1)) + x;
    }

    public static int makeNumber(int x, int y) {
        if (x > y) throw new AssertionError();
        return rand.nextInt(y-x+1) + x;
    }

    public static float makeFloat(float x, float y, float unit) {
        int xInt = (int)(x / unit + 0.5);
        int yInt = (int)(y / unit + 0.5);
        int n = makeNumber(xInt, yInt);
        return n * unit;
    }

    public static String makeString(String alphabet, int x, int y) {
        char[] alphabetArray = alphabet.toCharArray();
        int alphabetLen = alphabetArray.length;
        int len = makeNumber(x, y);
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < len; i++) {
            buf.append(alphabetArray[rand.nextInt(alphabetLen)]);
        }
        return buf.toString();
    }

    public static String makeAlphaString(int x, int y) {
        String ALPHA_NUM = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        return makeString(ALPHA_NUM, x, y);
    }

    public static String makeNumString(int x, int y) {
        String NUM = "0123456789";
        return makeString(NUM, x, y);
    }

    public static String fillOriginal(String s) {
        int pos = makeNumber(0, s.length()-8);
        return s.substring(0, pos) + "ORIGINAL" + s.substring(pos + 8);
    }

    public static String makeZip() {
        return makeNumString(4, 4) + "11111";
    }

    public static ArrayList<Integer> makePermutation(int n) {
        ArrayList<Integer> perm = new ArrayList<>(n);
        for (int i = 1; i <= n; i++)
            perm.add(i);
        Collections.shuffle(perm);
        return perm;
    }
}
