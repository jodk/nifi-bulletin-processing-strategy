package com.sugon.bulletin.processingstrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class AbstractHistoryHandler implements HistoryHandler{
    private Logger logger = LoggerFactory.getLogger(AbstractHistoryHandler.class);
    private int historyDay;
    private ScheduledExecutorService scheduledExecutorService = null;

    /**
     * 历史公告的处理方式
     * @return
     */
    public abstract Runnable worker(int maxDay);

    @Override
    public void handle(int maxDay){
        this.historyDay = maxDay;
        if(null == scheduledExecutorService){
            scheduledExecutorService = Executors.newScheduledThreadPool(1);
        }
        //每隔一小时处理一次
        int period = 60*60;
        scheduledExecutorService.scheduleAtFixedRate(worker(historyDay), 1, period, TimeUnit.SECONDS);
        logger.info("公告保存的历史数据处理线程启动成功，每隔{}秒调度一次...",period);
    }
    @Override
    public void destroy(){
        if(null != scheduledExecutorService){
            scheduledExecutorService.shutdown();
        }
    }
}
