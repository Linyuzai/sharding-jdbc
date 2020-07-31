package com.github.linyuzai.shardingjdbc;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * @author tangh
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import({ShardingJdbcAutoConfiguration.class})
@EnableConfigurationProperties(ShardingJdbcProperties.class)
public @interface EnableShardingJdbc {
}
