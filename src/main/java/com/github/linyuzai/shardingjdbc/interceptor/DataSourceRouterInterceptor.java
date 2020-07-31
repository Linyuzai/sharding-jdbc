package com.github.linyuzai.shardingjdbc.interceptor;

import com.github.linyuzai.shardingjdbc.DataSourceRouter;
import com.github.linyuzai.shardingjdbc.ShardingJdbc;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.SystemMetaObject;
import org.apache.shardingsphere.api.hint.HintManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.List;
import java.util.Properties;

@Intercepts({
        @Signature(method = "prepare", type = StatementHandler.class, args = {Connection.class, Integer.class})
})
public class DataSourceRouterInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceRouterInterceptor.class);

    private CCJSqlParserManager parser = new CCJSqlParserManager();

    private DataSourceRouter dataSourceRouter;

    public DataSourceRouterInterceptor(DataSourceRouter dataSourceRouter) {
        this.dataSourceRouter = dataSourceRouter;
    }

    @Override
    public Object intercept(Invocation invocation) throws Throwable {
        StatementHandler target = realTarget(invocation.getTarget());
        String sql = target.getBoundSql().getSql();
        Statement statement = parser.parse(new StringReader(sql));
        TablesNamesFinder finder = new TablesNamesFinder();
        List<String> tables = finder.getTableList(statement);
        boolean isHint = false;
        for (String key : dataSourceRouter.getMappings().keySet()) {
            if (tables.contains(key)) {
                logger.info("Found hint table => {}", key);
                isHint = true;
                break;
            }
        }
        if (isHint) {
            HintManager.getInstance().setDatabaseShardingValue(ShardingJdbc.hintValue);
            if (ShardingJdbc.loggerEnabled) {
                logger.info("Set hint value => {}", ShardingJdbc.hintValue);
            }
        }
        try {
            return invocation.proceed();
        } catch (Throwable e) {
            if (isHint) {
                HintManager.clear();
                if (ShardingJdbc.loggerEnabled) {
                    logger.info("Remove hint value");
                }
            }
            throw e;
        }
    }

    @Override
    public Object plugin(Object target) {
        if (target instanceof StatementHandler) {
            return Plugin.wrap(target, this);
        }
        return target;
    }

    @Override
    public void setProperties(Properties properties) {

    }

    @SuppressWarnings("unchecked")
    public <T> T realTarget(Object target) {
        if (Proxy.isProxyClass(target.getClass())) {
            MetaObject metaObject = SystemMetaObject.forObject(target);
            return realTarget(metaObject.getValue("h.target"));
        }
        return (T) target;
    }
}
