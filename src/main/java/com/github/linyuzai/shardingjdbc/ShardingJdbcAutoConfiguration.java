package com.github.linyuzai.shardingjdbc;

import com.github.linyuzai.shardingjdbc.algorithm.CombinedShardingAlgorithm;
import com.github.linyuzai.shardingjdbc.algorithm.DataSourceRouterShardingAlgorithm;
import com.github.linyuzai.shardingjdbc.interceptor.DataSourceRouterInterceptor;
import com.github.linyuzai.shardingjdbc.schedule.DynamicTableCreator;
import com.github.linyuzai.shardingjdbc.schedule.DynamicTableCreatorRunner;
import org.apache.shardingsphere.api.config.sharding.KeyGeneratorConfiguration;
import org.apache.shardingsphere.api.config.sharding.ShardingRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.TableRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.HintShardingStrategyConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.ShardingStrategyConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.StandardShardingStrategyConfiguration;
import org.apache.shardingsphere.shardingjdbc.api.ShardingDataSourceFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;

public class ShardingJdbcAutoConfiguration {

    @Autowired
    private Map<String, DataSource> dataSourceMap;

    @Autowired(required = false)
    private List<TableRuleConfiguration> tableRuleConfigurations;

    @Autowired(required = false)
    private List<ShardingTable> shardingTables;

    @Autowired(required = false)
    private DataSourceRouter dataSourceRouter;

    @Autowired
    private ShardingJdbcProperties properties;

    @Primary
    @Bean(name = ShardingJdbc.DATA_SOURCE_NAME, destroyMethod = "close")
    public DataSource autoShardingDataSource() throws SQLException {
        Map<String, TableRuleConfiguration> tableRules = new HashMap<>();
        ShardingRuleConfiguration configuration = new ShardingRuleConfiguration();
        if (tableRuleConfigurations != null) {
            for (TableRuleConfiguration trc : tableRuleConfigurations) {
                tableRules.put(trc.getLogicTable(), trc);
            }
        }
        if (shardingTables != null) {
            for (ShardingTable shardingTable : shardingTables) {
                Set<String> actual = new HashSet<>();
                for (String dataSource : dataSourceMap.keySet()) {
                    actual.add(dataSource + "." + shardingTable.getLogicTable());
                    for (String tableSuffix : shardingTable.getTableSuffixes()) {
                        actual.add(dataSource + "." + shardingTable.getLogicTable() + tableSuffix);
                    }
                }
                String actualDataNodes = String.join(",", actual);
                TableRuleConfiguration tableRuleConfiguration = new TableRuleConfiguration(shardingTable.getLogicTable(), actualDataNodes);
                tableRuleConfiguration.setKeyGeneratorConfig(new KeyGeneratorConfiguration(shardingTable.getKeyGenerator(), shardingTable.getKeyColumn()));
                CombinedShardingAlgorithm<?> shardingAlgorithm = shardingTable.getShardingAlgorithm();
                tableRuleConfiguration.setTableShardingStrategyConfig(new StandardShardingStrategyConfiguration(shardingTable.getShardingColumn(),
                        shardingAlgorithm, shardingAlgorithm));
                tableRules.put(tableRuleConfiguration.getLogicTable(), tableRuleConfiguration);
            }
        }
        if (dataSourceRouter != null) {
            configuration.setDefaultDataSourceName(dataSourceRouter.getDefaultDataSourceName());
            ShardingStrategyConfiguration ssc = new HintShardingStrategyConfiguration(
                    new DataSourceRouterShardingAlgorithm(dataSourceRouter));
            for (Map.Entry<String, Collection<String>> entry : dataSourceRouter.getMappings().entrySet()) {
                String logicTable = entry.getKey();
                TableRuleConfiguration exist = tableRules.get(logicTable);
                if (exist == null) {
                    Set<String> actual = new HashSet<>();
                    for (String value : entry.getValue()) {
                        actual.add(value + "." + logicTable);
                    }
                    String actualDataNodes = String.join(",", actual);
                    TableRuleConfiguration newRule = new TableRuleConfiguration(logicTable, actualDataNodes);
                    newRule.setDatabaseShardingStrategyConfig(ssc);
                    tableRules.put(logicTable, newRule);
                } else {
                    exist.setDatabaseShardingStrategyConfig(ssc);
                }
            }
        }
        configuration.getTableRuleConfigs().addAll(tableRules.values());
        return ShardingDataSourceFactory.createDataSource(dataSourceMap, configuration, properties.newProperties());
    }

    @Bean
    @ConditionalOnBean(DynamicTableCreator.class)
    public DynamicTableCreatorRunner dynamicTableCreatorRunner() {
        return new DynamicTableCreatorRunner();
    }

    @Bean
    @ConditionalOnBean(DataSourceRouter.class)
    public DataSourceRouterInterceptor hintShardingAlgorithmInterceptor() {
        return new DataSourceRouterInterceptor(dataSourceRouter);
    }
}
