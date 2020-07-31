package com.github.linyuzai.shardingjdbc;

import com.github.linyuzai.shardingjdbc.util.Func;

import java.util.*;

public class DataSourceRouter {

    private Map<String, Collection<String>> mapping = new HashMap<>();

    private String defaultDataSourceName;

    public DataSourceRouter(String defaultDataSourceName) {
        this.defaultDataSourceName = defaultDataSourceName;
    }

    public DataSourceRouter add(String dsName, Func<TableRouter> router) {
        TableRouter tr = new TableRouter();
        router.apply(tr);
        for (String tableName : tr.tables) {
            Collection<String> dss = mapping.computeIfAbsent(tableName, k -> new HashSet<>());
            dss.add(dsName);
        }
        return this;
    }

    public String getDefaultDataSourceName() {
        return defaultDataSourceName;
    }

    public void setDefaultDataSourceName(String defaultDataSourceName) {
        this.defaultDataSourceName = defaultDataSourceName;
    }

    public Collection<String> getMapping(String tableName) {
        return mapping.getOrDefault(tableName, Collections.emptyList());
    }

    public Map<String, Collection<String>> getMappings() {
        return mapping;
    }

    public static class TableRouter {
        private Collection<String> tables = new HashSet<>();

        public TableRouter add(String... tableNames) {
            add(Arrays.asList(tableNames));
            return this;
        }

        public TableRouter add(Collection<? extends String> tableNames) {
            tables.addAll(tableNames);
            return this;
        }
    }
}
