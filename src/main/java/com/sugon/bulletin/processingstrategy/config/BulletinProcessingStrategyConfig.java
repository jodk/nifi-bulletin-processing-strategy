package com.sugon.bulletin.processingstrategy.config;

import org.apache.nifi.util.NiFiProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class BulletinProcessingStrategyConfig {
    public static final String CONFIG_PREFIX = "nifi.bulletin.processing.strategy.";
    public static final String CONFIG_STRATEGY_STORE_PREFIX = "nifi.bulletin.processing.strategy.store.";
    public static final String STRATEGY_IMPLEMENTATION = "nifi.bulletin.processing.strategy.implementation";
    public static final String STRATEGY_DIRECTORY = "nifi.bulletin.processing.strategy.directory";
    public static final String STRATEGY_QUEUE = "nifi.bulletin.processing.strategy.queue";
    public static final String STRATEGY_PERSISTENT_MAX_HISTORY_DAY = "nifi.bulletin.processing.strategy.persistent.max.history.day";
    public static final String STRATEGY_STORE = "nifi.bulletin.processing.strategy.store";

    private String implementation;
    private String directory;
    private String queueSize;
    private String maxHistoryDay;
    private String storeType;
    private Map<String,String> storeProperties = new HashMap<>();

    public static BulletinProcessingStrategyConfig create(NiFiProperties niFiProperties){
        BulletinProcessingStrategyConfig config = new BulletinProcessingStrategyConfig();
        config.implementation = niFiProperties.getProperty(STRATEGY_IMPLEMENTATION);
        config.directory = niFiProperties.getProperty(STRATEGY_DIRECTORY);
        config.maxHistoryDay = niFiProperties.getProperty(STRATEGY_PERSISTENT_MAX_HISTORY_DAY);
        config.queueSize = niFiProperties.getProperty(STRATEGY_QUEUE);
        config.storeType = niFiProperties.getProperty(STRATEGY_STORE);
        config.storeProperties =storePropertyMap(niFiProperties,config.storeType);
        return config;
    }
    private static Map<String,String> storePropertyMap(NiFiProperties niFiProperties,String storeType){
        Map<String,String> storeConfigMap = new HashMap<>();
        Set<String> propertyKeys = niFiProperties.getPropertyKeys();
        for (String key : propertyKeys){
            if(key.startsWith(CONFIG_STRATEGY_STORE_PREFIX)){
                String subKey = key.replace(CONFIG_STRATEGY_STORE_PREFIX+storeType+".","");
                storeConfigMap.put(subKey,niFiProperties.getProperty(key));
            }
        }
        return storeConfigMap;
    }
    public String getImplementation() {
        return implementation;
    }

    public String getDirectory() {
        return directory;
    }

    public String getQueueSize() {
        return queueSize;
    }

    public String getMaxHistoryDay() {
        return maxHistoryDay;
    }

    public String getStoreType() {
        return storeType;
    }

    public Map<String, String> getStoreProperties() {
        return storeProperties;
    }
}
