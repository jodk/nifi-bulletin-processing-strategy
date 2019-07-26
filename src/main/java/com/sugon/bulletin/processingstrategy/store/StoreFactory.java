package com.sugon.bulletin.processingstrategy.store;

import com.sugon.bulletin.processingstrategy.exception.ProcessingStrategyException;
import com.sugon.bulletin.processingstrategy.store.mysql.MysqlStoreServiceImpl;

import java.util.Map;

public class StoreFactory {
    public static StoreService get(StoreType type, Map<String, String> config) {
        switch (type) {
            case MYSQL:
                MysqlStoreServiceImpl mysqlStoreService = new MysqlStoreServiceImpl();
                mysqlStoreService.config(config);
                return mysqlStoreService;
            case ES:
            default:
                throw new ProcessingStrategyException("不支持的存储类型:%s", type);
        }
    }
}
