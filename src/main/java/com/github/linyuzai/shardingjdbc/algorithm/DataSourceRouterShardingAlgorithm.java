package com.github.linyuzai.shardingjdbc.algorithm;

import com.github.linyuzai.shardingjdbc.DataSourceRouter;
import com.github.linyuzai.shardingjdbc.ShardingJdbc;
import org.apache.shardingsphere.api.hint.HintManager;
import org.apache.shardingsphere.api.sharding.hint.HintShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.hint.HintShardingValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class DataSourceRouterShardingAlgorithm implements HintShardingAlgorithm<String> {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceRouterShardingAlgorithm.class);

    private DataSourceRouter dataSourceRouter;

    public DataSourceRouterShardingAlgorithm(DataSourceRouter dataSourceRouter) {
        this.dataSourceRouter = dataSourceRouter;
    }

    public DataSourceRouter getDataSourceRouter() {
        return dataSourceRouter;
    }

    public void setDataSourceRouter(DataSourceRouter dataSourceRouter) {
        this.dataSourceRouter = dataSourceRouter;
    }

    @Override
    public Collection<String> doSharding(Collection<String> availableTargetNames, HintShardingValue<String> shardingValue) {
        if (ShardingJdbc.loggerEnabled) {
            logger.info("Hint sharding");
        }
        HintManager.clear();
        if (ShardingJdbc.loggerEnabled) {
            logger.info("Remove hint value");
        }
        return dataSourceRouter.getMapping(shardingValue.getLogicTableName());
    }
}
