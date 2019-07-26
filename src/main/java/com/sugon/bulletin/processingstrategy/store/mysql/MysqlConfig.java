package com.sugon.bulletin.processingstrategy.store.mysql;

import com.sugon.bulletin.processingstrategy.exception.ProcessingStrategyException;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

public class MysqlConfig {
    private static final String TABLE_NAME_KEY = "tablename";
    private static final String URL_KEY = "url";
    private static final String DRIVER_CLASS_NAME_KEY = "driverClassName";
    private static final String INITIAL_SIZE_KEY = "initialSize";
    private static final String MAX_ACTIVE_KEY = "maxActive";
    private static final String MAX_WAIT_KEY = "maxWait";
    private static final String MIN_IDLE_KEY = "minIdle";

    private static final String defaultDriverClass = "com.mysql.jdbc.Driver";
    private static final String defaultInitialSize = "5";
    private static final String defaultMaxActive = "10";
    private static final String defaultMaxWait = "60000";
    private static final String defaultMinIdle = "5";

    public static Map<String,String> createDruidProperties(Map<String,String> config){
        Map<String,String> druidProperties = new HashMap<>();
        druidProperties.putAll(config);
        String url = config.get(URL_KEY);
        String driver = config.get(DRIVER_CLASS_NAME_KEY);
        String initialSize = config.get(INITIAL_SIZE_KEY);
        String maxActive = config.get(MAX_ACTIVE_KEY);
        String maxWait = config.get(MAX_WAIT_KEY);
        String minIdle = config.get(MIN_IDLE_KEY);

        if(StringUtils.isBlank(url)){
            throw new ProcessingStrategyException("mysql连接url不能为空");
        }
        if(StringUtils.isBlank(driver)){
            druidProperties.put(DRIVER_CLASS_NAME_KEY, defaultDriverClass);
        }
        if(StringUtils.isBlank(initialSize)){
            druidProperties.put(INITIAL_SIZE_KEY,defaultInitialSize);
        }
        if(StringUtils.isBlank(maxActive)){
            druidProperties.put(MAX_ACTIVE_KEY,defaultMaxActive);
        }
        if(StringUtils.isBlank(maxWait)){
            druidProperties.put(MAX_WAIT_KEY,defaultMaxWait);
        }
        if(StringUtils.isBlank(minIdle)){
            druidProperties.put(MIN_IDLE_KEY,defaultMinIdle);
        }
        return druidProperties;
    }
}
