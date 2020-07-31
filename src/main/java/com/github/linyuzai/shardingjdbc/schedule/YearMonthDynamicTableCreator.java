package com.github.linyuzai.shardingjdbc.schedule;

import com.github.linyuzai.shardingjdbc.util.MonthUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;

public class YearMonthDynamicTableCreator extends DynamicTableCreator {

    private static final Logger logger = LoggerFactory.getLogger(YearMonthDynamicTableCreator.class);

    private boolean ymMark = false;

    private int monthLength;

    private Callback callback;

    public YearMonthDynamicTableCreator(String tableName, int monthLength) {
        super(tableName);
        this.monthLength = monthLength;
    }

    public YearMonthDynamicTableCreator(String tableName, String cron, int monthLength) {
        super(tableName, cron);
        this.monthLength = monthLength;
    }

    public void setCallback(Callback callback) {
        this.callback = callback;
    }

    public Callback getCallback() {
        return callback;
    }

    public void setMonthLength(int monthLength) {
        this.monthLength = monthLength;
    }

    public int getMonthLength() {
        return monthLength;
    }

    @Override
    public String getNewTableName(String tableName, int monthPlus) {
        String suffix = MonthUtils.getSuffix(LocalDate.now().plusMonths(monthPlus), monthLength, "_");
        return tableName.concat("_").concat(suffix);
    }

    @Override
    protected Exception afterCreateTable(Statement statement, String tableName) {
        if (!ymMark) {
            ymMark = true;
            try (ResultSet rs = statement.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_name like '" + tableName + "%'")) {
                int year = 10000;
                int month = 100;
                while (rs.next()) {
                    String table = rs.getString(1);
                    if (!tableName.equals(table)) {
                        String[] ym = table.substring(tableName.length() + 1).split("_");
                        int y = Integer.parseInt(ym[0]);
                        int m = Integer.parseInt(ym[1]);
                        if (y < year) {
                            year = y;
                            month = m;
                        } else if (y == year) {
                            if (m < month) {
                                month = m;
                            }
                        }
                    }
                }
                if (callback != null) {
                    callback.onYearMonth(year, month);
                }
            } catch (Exception e) {
                logger.error("query table " + tableName + "% failure", e);
                return e;
            }
        }
        return null;
    }

    public interface Callback {
        void onYearMonth(int year, int month);
    }
}
