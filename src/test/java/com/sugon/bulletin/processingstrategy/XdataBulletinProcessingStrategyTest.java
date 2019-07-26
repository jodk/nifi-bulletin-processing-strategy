package com.sugon.bulletin.processingstrategy;

import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.properties.StandardNiFiProperties;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class XdataBulletinProcessingStrategyTest {
    private NiFiProperties niFiProperties;

    @Before
    public void setUp() throws Exception {
        String configFile = "D:\\projects\\src\\XData\\Trunk\\Data Ingest1.0\\XData-nifi-bulletin-processingstrategy\\src\\main\\resources\\conf\\nifi.properties";
        NiFiPropertiesLoader loader = new NiFiPropertiesLoader();

        niFiProperties = loader.load(new File(configFile));
    }

    @Test
    public void update() {
    }

    @Test
    public void storeService() {
        XdataBulletinProcessingStrategy strategy = new XdataBulletinProcessingStrategy(niFiProperties);
    }
}