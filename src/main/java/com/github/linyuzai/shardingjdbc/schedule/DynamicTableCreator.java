package com.github.linyuzai.shardingjdbc.schedule;

import com.github.linyuzai.shardingjdbc.ShardingJdbc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public abstract class DynamicTableCreator implements SchedulingConfigurer, ApplicationContextAware {

    private static final Logger logger = LoggerFactory.getLogger(DynamicTableCreator.class);

    private String cron = "0 0 1 1 * ? ";//每个月1号的1点执行

    private ApplicationContext applicationContext;

    private DataSource dataSource;

    private String tableName;

    public DynamicTableCreator(String tableName) {
        this.tableName = tableName;
    }

    public DynamicTableCreator(String tableName, String cron) {
        this.tableName = tableName;
        this.cron = cron;
    }

    @Override
    public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
        taskRegistrar.addCronTask(() -> createTable(1), cron);
    }

    protected Exception createTable(int... monthPlus) {
        if (dataSource == null) {
            dataSource = applicationContext.getBean(ShardingJdbc.DATA_SOURCE_NAME, DataSource.class);
        }
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("show create table " + tableName + ";")) {
            resultSet.next();
            String createSql = resultSet.getString(2);
            for (int plus : monthPlus) {
                String newTableSql = createSql.replaceAll(tableName, getNewTableName(tableName, plus));
                int index = newTableSql.indexOf("`");
                statement.execute("CREATE TABLE IF NOT EXISTS " + newTableSql.substring(index));
            }
            return afterCreateTable(statement, tableName);
        } catch (Exception e) {
            logger.error("create table " + tableName + " failure", e);
            return e;
        }
    }



    protected Exception afterCreateTable(Statement statement, String tableName) {
        return null;
    }

    public abstract String getNewTableName(String tableName, int monthPlus);

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
