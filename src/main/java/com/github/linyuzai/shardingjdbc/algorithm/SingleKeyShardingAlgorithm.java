package com.github.linyuzai.shardingjdbc.algorithm;

import com.github.linyuzai.shardingjdbc.ShardingJdbc;
import com.google.common.collect.Range;
import org.apache.shardingsphere.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;

public abstract class SingleKeyShardingAlgorithm<T extends Comparable<T>> implements CombinedShardingAlgorithm<T> {

    private static final Logger logger = LoggerFactory.getLogger(SingleKeyShardingAlgorithm.class);

    @Override
    public String doSharding(Collection<String> availableTargetNames, PreciseShardingValue<T> shardingValue) {
        if (ShardingJdbc.loggerEnabled) {
            logger.info("Precise sharding => {} => {} => {}",
                    shardingValue.getLogicTableName(),
                    shardingValue.getColumnName(),
                    shardingValue.getValue());
        }
        return doShardingSingleKey(shardingValue.getLogicTableName(), shardingValue.getValue());
    }

    @Override
    public Collection<String> doSharding(Collection<String> availableTargetNames, RangeShardingValue<T> shardingValue) {
        Range<T> range = shardingValue.getValueRange();
        T lowerValue = range.hasLowerBound() ? range.lowerEndpoint() : null;
        T upperValue = range.hasUpperBound() ? range.upperEndpoint() : null;
        if (ShardingJdbc.loggerEnabled) {
            logger.info("Range sharding => {} => {} => {} => {}",
                    shardingValue.getLogicTableName(),
                    shardingValue.getColumnName(),
                    lowerValue, upperValue);
        }
        return new HashSet<>(doShardingSingleKey(shardingValue.getLogicTableName(), lowerValue, upperValue));
    }

    public abstract String doShardingSingleKey(String targetName, T value);

    public abstract Collection<String> doShardingSingleKey(String targetName, T lowerValue, T upperValue);
}
