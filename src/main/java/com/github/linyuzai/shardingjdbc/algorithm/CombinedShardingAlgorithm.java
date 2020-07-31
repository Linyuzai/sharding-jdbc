package com.github.linyuzai.shardingjdbc.algorithm;

import org.apache.shardingsphere.api.sharding.standard.PreciseShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingAlgorithm;

public interface CombinedShardingAlgorithm<T extends Comparable<?>> extends PreciseShardingAlgorithm<T>, RangeShardingAlgorithm<T> {
}
