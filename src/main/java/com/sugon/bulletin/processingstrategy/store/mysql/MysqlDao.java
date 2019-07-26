package com.sugon.bulletin.processingstrategy.store.mysql;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.sugon.bulletin.processingstrategy.exception.ProcessingStrategyException;
import org.apache.nifi.reporting.Bulletin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class MysqlDao {
    private Logger logger = LoggerFactory.getLogger(MysqlDao.class);
    private DataSource dataSource;
    private final String insertSql = "insert into sys_bulletin(" +
            "id," +
            "at_time," +
            "node_address," +
            "level," +
            "category," +
            "message," +
            "group_id," +
            "group_name," +
            "source_id," +
            "source_name," +
            "source_type) values(?,?,?,?,?,?,?,?,?,?,?)";

    private final String deleteByAtTimeSql = "delete from sys_bulletin where at_time <= ?";

    public MysqlDao(Map<String, String> properties) {
        try {
            dataSource = DruidDataSourceFactory.createDataSource(properties);
            ((DruidDataSource)dataSource).setName("dataSource-公告外部系统(MYSQL)数据源");
            testConnection();
        } catch (Exception e) {
            throw new ProcessingStrategyException("创建MYSQL存储介质的连接失败", e);
        }
    }

    public int batchInsert(List<Bulletin> records) {
        int size = records.size();
        try (Connection con = dataSource.getConnection()) {
            con.setAutoCommit(false);
            PreparedStatement ps = con.prepareStatement(insertSql);
            for (int i = 0; i < size; i++) {
                int index = 0;
                Bulletin bulletin = records.get(i);
                ps.setLong(++index, bulletin.getId());
                ps.setTimestamp(++index, new Timestamp(bulletin.getTimestamp().getTime()));
                ps.setString(++index, bulletin.getNodeAddress());
                ps.setString(++index, bulletin.getLevel());
                ps.setString(++index, bulletin.getCategory());
                ps.setString(++index, bulletin.getMessage());
                ps.setString(++index, bulletin.getGroupId());
                ps.setString(++index, bulletin.getGroupName());
                ps.setString(++index, bulletin.getSourceId());
                ps.setString(++index, bulletin.getSourceName());
                ps.setString(++index, bulletin.getSourceType().name());

                ps.addBatch();
            }
            ps.executeBatch();
            con.commit();
            ps.clearBatch();
        } catch (Exception e) {
            throw new ProcessingStrategyException("公告批量写入mysql失败", e);
        }
        return size;
    }

    public void deleteBeforeAtTime(Date atTime){
        try (Connection con = dataSource.getConnection()) {
            PreparedStatement ps = con.prepareStatement(deleteByAtTimeSql);
            ps.setTimestamp(1,new Timestamp(atTime.getTime()));
            ps.execute();
        }catch (Exception e){
            throw new ProcessingStrategyException("公告删除失败", e);
        }
    }
    private void testConnection() throws Exception{
        try (Connection connection = dataSource.getConnection()){
            logger.info("公告存储介质mysql连接正常");
        }
    }
}
