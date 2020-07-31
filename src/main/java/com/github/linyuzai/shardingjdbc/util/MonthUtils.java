package com.github.linyuzai.shardingjdbc.util;

import java.time.LocalDate;

public class MonthUtils {

    public static int calculate(int month, int length) {
        return ((month - 1) / length) * length + 1;
    }

    public static String getSuffix(LocalDate localDate, int length, String s) {
        int year = localDate.getYear();
        int month = calculate(localDate.getMonthValue(), length);
        return year + s + (month < 10 ? "0" + month : month);
    }
}
