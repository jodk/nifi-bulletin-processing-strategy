package com.sugon.bulletin.processingstrategy;

import com.sugon.bulletin.processingstrategy.config.BulletinProcessingStrategyConfig;
import com.sugon.bulletin.processingstrategy.exception.ProcessingStrategyException;
import com.sugon.bulletin.processingstrategy.store.StoreFactory;
import com.sugon.bulletin.processingstrategy.store.StoreService;
import com.sugon.bulletin.processingstrategy.store.StoreType;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.events.BulletinProcessingStrategy;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class XdataBulletinProcessingStrategy implements BulletinProcessingStrategy {
    private Logger logger = LoggerFactory.getLogger(XdataBulletinProcessingStrategy.class);

    private NiFiProperties niFiProperties;
    private BulletinProcessingStrategyConfig config;

    private ArrayBlockingQueue<Bulletin> queue;
    private StoreService storeService;
    private ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private HistoryHandler historyHandler;
    private boolean normal = true;

    private static AtomicInteger discardCount = new AtomicInteger(0);
    private static int discardPrintThreshold = 200;
    private static int consumerMaxElements = 99;



    public XdataBulletinProcessingStrategy() {

    }
    public XdataBulletinProcessingStrategy(NiFiProperties niFiProperties) {
        init(niFiProperties);
    }
    private void init(NiFiProperties niFiProperties) {
        this.normal = true;
        this.niFiProperties = niFiProperties;
        this.config = BulletinProcessingStrategyConfig.create(niFiProperties);
        initQueue();
        initStoreService();
        triggerHistoryHandler();
        initConsumerManager();
        initShutdownHook();
    }

    @Override
    public void update(Bulletin bulletin) {
        if (!normal) {
            return;
        }

        boolean offer = queue.offer(bulletin);
        if (!offer) {
            if (discardCount.incrementAndGet() > discardPrintThreshold) {
                logger.warn("公告队列繁忙，公告被丢弃{}条", discardPrintThreshold);
                discardCount.set(0);
            }
        }
    }

    private void initQueue() {
        int size = 10000;
        String queueSize = config.getQueueSize();
        if (StringUtils.isNotBlank(queueSize)) {
            try {
                size = Integer.parseInt(queueSize.trim());
            } catch (Exception e) {
                logger.warn("公告队列参数{}配置异常", BulletinProcessingStrategyConfig.STRATEGY_QUEUE);
            }
        }
        logger.info("公告队列大小值:{}", size);
        queue = new ArrayBlockingQueue<>(size);
    }

    private void initStoreService() {
        String storeTypeName = config.getStoreType();
        logger.debug("公告存储介质类型:{}", storeTypeName);
        try {
            StoreType storeType = StoreType.valueOf(storeTypeName.trim());
            storeService = StoreFactory.get(storeType, config.getStoreProperties());
        } catch (Exception e) {
            throw new ProcessingStrategyException(e, "公告存储介质[%s]配置异常,介质类型只识别:[%s]"
                    , storeTypeName, StringUtils.join(StoreType.values(), ","));
        }
        logger.info("初始化公告存储介质服务成功...");
    }

    private void initConsumerManager() {
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                List<Bulletin> outputs = new ArrayList<>();
                while (null != queue) {
                    try {
                        //当队列中无数据时阻塞
                        Bulletin first = queue.take();
                        outputs.add(first);
                        queue.drainTo(outputs, consumerMaxElements);
                        long savedCount = storeService.save(outputs);
                        logger.debug("存储公告{}条成功", savedCount);
                    } catch (Exception e) {
                        logger.error("公告从队列消费失败，请检查存储介质:{}", config.getStoreType(),e);
                        int size = outputs.size();
                        logger.warn("因存储介质异常,公告有{}条被丢弃", size);
                    } finally {
                        outputs.clear();
                    }
                }
            }
        };
        scheduledExecutorService.scheduleAtFixedRate(runnable, 1, 1, TimeUnit.SECONDS);
        logger.info("公告队列消费线程启动成功...");
    }

    private void triggerHistoryHandler(){
        if(null != this.config.getMaxHistoryDay()){
            try {
                int maxDay = Integer.parseInt(this.config.getMaxHistoryDay());
                if(maxDay<=0){
                    throw new ProcessingStrategyException("公告历史数据保存时间不能小于1天");
                }
                historyHandler = storeService.historyHandler();
                historyHandler.handle(maxDay);
            }catch (Exception e){
                logger.warn("公告历史数据保存时间[{}]设置异常,不能触发公告历史的处理程序",this.config.getMaxHistoryDay(),e);
            }
        }



    }

    private void initShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("bulletin processing strategy shut down hook trigger...");
                normal = false;
                while (null != queue && !queue.isEmpty()) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        //nothing
                    }
                }
                scheduledExecutorService.shutdown();
                if(null != historyHandler){
                    historyHandler.destroy();
                }
            }
        }));
    }
}
