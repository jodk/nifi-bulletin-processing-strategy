package com.sugon.bulletin.nifi.sources;

import org.apache.nifi.events.BulletinFactory;
import org.apache.nifi.events.ComponentBulletin;
import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.reporting.Severity;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class XdataVolatileBulletinRepositoryTest {

    private XdataVolatileBulletinRepository repository;

    @Before
    public void setUp() throws Exception {
        String configFile = "D:\\projects\\src\\XData\\Trunk\\Data Ingest1.0\\XData-nifi-bulletin-processingstrategy\\src\\main\\resources\\conf\\nifi.properties";
        NiFiPropertiesLoader loader = new NiFiPropertiesLoader();
        repository = new XdataVolatileBulletinRepository(loader.load(new File(configFile)));
    }

    @Test
    public void addBulletin() {
        for (int i=0;i<10009;i++){
            String groupId = "111aaa"+(i%3);
            String groupName = "向导作业-1"+(i%3);
            String sourceId = "222bbb"+(i%5);
            String sourceName = "中文put2hive"+(i%10);
            Bulletin bulletin = BulletinFactory.createBulletin(groupId, sourceId,
                    ComponentType.PROCESSOR, sourceName,
                    "log msg", Severity.ERROR.name(), "this is message" + i);
            if(i%500 == 2){
                System.out.println("暂停2秒....");
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            repository.addBulletin(bulletin);
        }
        try {
            Thread.sleep(500000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}