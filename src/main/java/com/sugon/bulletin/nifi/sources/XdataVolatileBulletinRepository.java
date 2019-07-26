/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sugon.bulletin.nifi.sources;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.events.BulletinProcessingStrategy;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.events.XdataBulletinExtensionDiscoveringManager;
import org.apache.nifi.events.XdataBulletinProcessingStrategyBundle;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;

public class XdataVolatileBulletinRepository extends VolatileBulletinRepository {
    private Logger logger = LoggerFactory.getLogger(org.apache.nifi.events.XdataVolatileBulletinRepository.class);
    private NiFiProperties nifiProperties;
    private String nodeAddress;

    public XdataVolatileBulletinRepository(NiFiProperties nifiProperties) {
        this.nifiProperties = nifiProperties;
        this.nodeAddress = localhost();
        initXdataBulletinProcessingStrategy();
    }

    private volatile BulletinProcessingStrategy xdataBulletinProcessingStrategy;

    @Override
    public void addBulletin(final Bulletin bulletin) {
        super.addBulletin(bulletin);

        if (null != xdataBulletinProcessingStrategy) {
            //write to queue and add nodeAddress
            bulletin.setNodeAddress(nodeAddress);
            xdataBulletinProcessingStrategy.update(bulletin);
        }
    }

    /**
     * 为了不影响nifi正常运行需要捕获异常
     */
    private void initXdataBulletinProcessingStrategy() {
        try {
            Set<Bundle> bundles = XdataBulletinProcessingStrategyBundle.create(nifiProperties);
            ExtensionDiscoveringManager extensionManager = new XdataBulletinExtensionDiscoveringManager();
            extensionManager.discoverExtensions(bundles);
            final String implementationClassName = nifiProperties.getProperty("nifi.bulletin.processing.strategy.implementation");
            if (implementationClassName == null) {
                throw new RuntimeException("Cannot create Log Repository because the NiFi Properties is missing the following property: "
                        + NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS);
            }
            xdataBulletinProcessingStrategy = NarThreadContextClassLoader
                    .createInstance(extensionManager, implementationClassName, BulletinProcessingStrategy.class, nifiProperties);

        } catch (Exception e) {
            logger.error("初始化公告策略失败，公告不能被写入到外部系统！", e);
        }
    }

    private String localhost() {
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            String hostAddress = localHost.getHostAddress();
            if (null == hostAddress) {
                hostAddress = localHost.getHostName();
            }
            return hostAddress;
        } catch (UnknownHostException e) {
            //nothing
        }
        return "unknown";
    }
}
