package com.github.linyuzai.shardingjdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@ConfigurationProperties("sharding-jdbc")
public class ShardingJdbcProperties {

    private static final Logger logger = LoggerFactory.getLogger(ShardingJdbcProperties.class);

    /**
     * @property sql.show
     * @description 是否在日志中打印 SQL。
     * 打印 SQL 可以帮助开发者快速定位系统问题。日志内容包含：逻辑 SQL，真实 SQL 和 SQL 解析结果。
     * 如果开启配置，日志将使用 Topic ShardingSphere-SQL，日志级别是 INFO。
     * @default false
     */

    /**
     * @property sql.simple
     * @description 是否在日志中打印简单风格的 SQL。
     * @default false
     */

    /**
     * @property executor.size
     * @description 用于设置任务处理线程池的大小。
     * 每个 ShardingSphereDataSource 使用一个独立的线程池，同一个 JVM 的不同数据源不共享线程池。
     * @default infinite
     */

    /**
     * @property max.connections.size.per.query
     * @description 一次查询请求在每个数据库实例中所能使用的最大连接数。
     * @default 1
     */

    /**
     * @property check.table.metadata.enabled
     * @description 是否在程序启动和更新时检查分片元数据的结构一致性。
     * @default false
     */

    /**
     * @property query.with.cipher.column
     * @description 是否使用加密列进行查询。在有原文列的情况下，可以使用原文列进行查询。
     * @default true
     */

    private Map<String, String> properties = new HashMap<>();

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Properties newProperties() {
        Properties p = new Properties();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            p.setProperty(key, value);
            logger.info("Sharding jdbc property => {} : {}", key, value);
        }
        String v = properties.get("sql.show");
        if (v == null) {
            p.setProperty("sql.show", "true");
            logger.info("Sharding jdbc property default => sql.show : true");
        }
        return p;
    }
}
