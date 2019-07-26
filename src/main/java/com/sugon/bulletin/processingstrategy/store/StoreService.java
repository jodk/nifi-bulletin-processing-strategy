package com.sugon.bulletin.processingstrategy.store;

import com.sugon.bulletin.processingstrategy.AbstractHistoryHandler;
import org.apache.nifi.reporting.Bulletin;

import java.util.List;

public interface StoreService extends Configurable{

    /**
     * 保存一个公告
     * @param bulletin 公告
     */
    public void save(Bulletin bulletin);

    /**
     * 批量保存
     * @param bulletinList 公告列表
     * @return 返回保存的条数
     */
    public long save(List<Bulletin> bulletinList);

    /**
     * 存储的历史公告处理方式
     * @return
     */
    public AbstractHistoryHandler historyHandler();
}
