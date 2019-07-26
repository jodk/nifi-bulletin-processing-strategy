package com.sugon.bulletin.processingstrategy.store.mysql;

import com.sugon.bulletin.processingstrategy.AbstractHistoryHandler;
import com.sugon.bulletin.processingstrategy.store.StoreService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.Bulletin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class MysqlStoreServiceImpl implements StoreService {
    private Logger logger = LoggerFactory.getLogger(MysqlStoreServiceImpl.class);
    private MysqlDao dao;

    @Override
    public void save(Bulletin bulletin) {
        throw new ProcessException("not support.");
    }

    @Override
    public long save(List<Bulletin> bulletinList) {
        return dao.batchInsert(bulletinList);
    }

    private void deleteBeforeAtTime(Date atTime) {
        dao.deleteBeforeAtTime(atTime);
    }

    @Override
    public AbstractHistoryHandler historyHandler() {
        return new AbstractHistoryHandler() {
            @Override
            public Runnable worker(int maxDay) {
                return new Runnable() {
                    @Override
                    public void run() {
                        logger.debug("删除当前时间向前推{}天的公告历史....",maxDay);
                        Date current = new Date();
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime(current);
                        calendar.add(Calendar.DATE, -1 * maxDay);
                        try {
                            deleteBeforeAtTime(calendar.getTime());
                        } catch (Exception e) {
                            logger.warn("公告历史数据(小于{})删除失败",calendar.getTime(), e);
                        }
                    }
                };
            }
        };
    }

    @Override
    public void config(Map<String, String> map) {
        //map中是druid的配置
        Map<String, String> druidProperties = MysqlConfig.createDruidProperties(map);
        dao = new MysqlDao(druidProperties);
    }

}
