package com.github.linyuzai.shardingjdbc.algorithm;

import com.github.linyuzai.shardingjdbc.schedule.YearMonthDynamicTableCreator;
import com.github.linyuzai.shardingjdbc.util.MonthUtils;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class YearMonthShardingAlgorithm extends SingleKeyShardingAlgorithm<Date> {

    private LocalDate shardingDate;
    private int monthLength;

    public YearMonthShardingAlgorithm(int monthLength, int shardingYear, int shardingMonth) {
        this.monthLength = monthLength;
        setShardingDate(shardingYear, shardingMonth);
    }

    public YearMonthShardingAlgorithm(YearMonthDynamicTableCreator creator) {
        this.monthLength = creator.getMonthLength();
        creator.setCallback(this::setShardingDate);
    }

    public void setShardingDate(LocalDate shardingDate) {
        this.shardingDate = shardingDate;
    }

    public void setShardingDate(int shardingYear, int shardingMonth) {
        this.shardingDate = LocalDate.of(shardingYear, shardingMonth, 1);
    }

    public LocalDate getShardingDate() {
        return shardingDate;
    }

    public void setMonthLength(int monthLength) {
        this.monthLength = monthLength;
    }

    public int getMonthLength() {
        return monthLength;
    }

    @Override
    public String doShardingSingleKey(String targetName, Date value) {
        String suffix = MonthUtils.getSuffix(toLocal(value), monthLength, "_");
        return targetName.concat("_").concat(suffix);
    }

    @Override
    public Collection<String> doShardingSingleKey(String targetName, Date lowerValue, Date upperValue) {
        Set<String> targets = new HashSet<>();
        LocalDate now = LocalDate.now();
        LocalDate lowerLocalDate;
        LocalDate upperLocalDate;
        if (lowerValue == null) {
            lowerLocalDate = LocalDate.from(shardingDate);
        } else {
            lowerLocalDate = toLocal(lowerValue);
        }
        if (upperValue == null) {
            upperLocalDate = now.withDayOfMonth(1);
        } else {
            upperLocalDate = toLocal(upperValue);
        }
        lowerLocalDate = withBound(lowerLocalDate, now);
        upperLocalDate = withBound(upperLocalDate, now);
        while (lowerLocalDate.isBefore(upperLocalDate) || lowerLocalDate.isEqual(upperLocalDate)) {
            String suffix = MonthUtils.getSuffix(lowerLocalDate, monthLength, "_");
            targets.add(targetName.concat("_").concat(suffix));
            lowerLocalDate = lowerLocalDate.plusMonths(1);
        }
        return targets;
    }

    private LocalDate withBound(LocalDate localDate, LocalDate now) {
        if (localDate.isAfter(now)) {
            localDate = now.withDayOfMonth(1);
        }
        if (localDate.isBefore(shardingDate)) {
            localDate = LocalDate.from(shardingDate);
        }
        return localDate;
    }

    private LocalDate toLocal(Date date) {
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate().withDayOfMonth(1);
    }
}
