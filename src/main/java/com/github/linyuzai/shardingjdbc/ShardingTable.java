package com.github.linyuzai.shardingjdbc;

import com.github.linyuzai.shardingjdbc.algorithm.CombinedShardingAlgorithm;

import java.util.ArrayList;
import java.util.List;

public class ShardingTable {

    private String logicTable;

    private String keyColumn;

    private String keyGenerator = "SNOWFLAKE";

    private String shardingColumn;

    private List<String> tableSuffixes = new ArrayList<>();

    private CombinedShardingAlgorithm<?> shardingAlgorithm;

    public String getLogicTable() {
        return logicTable;
    }

    public ShardingTable logicTable(String logicTable) {
        this.logicTable = logicTable;
        return this;
    }

    public String getKeyColumn() {
        return keyColumn;
    }

    public ShardingTable keyColumn(String keyColumn) {
        this.keyColumn = keyColumn;
        return this;
    }

    public String getKeyGenerator() {
        return keyGenerator;
    }

    public ShardingTable keyGenerator(String keyGenerator) {
        this.keyGenerator = keyGenerator;
        return this;
    }

    public String getShardingColumn() {
        return shardingColumn;
    }

    public ShardingTable shardingColumn(String shardingColumn) {
        this.shardingColumn = shardingColumn;
        return this;
    }

    public List<String> getTableSuffixes() {
        return tableSuffixes;
    }

    public ShardingTable tableSuffixes(List<String> tableSuffixes) {
        this.tableSuffixes = tableSuffixes;
        return this;
    }

    public CombinedShardingAlgorithm<?> getShardingAlgorithm() {
        return shardingAlgorithm;
    }

    public ShardingTable setShardingAlgorithm(CombinedShardingAlgorithm<?> shardingAlgorithm) {
        this.shardingAlgorithm = shardingAlgorithm;
        return this;
    }
}
