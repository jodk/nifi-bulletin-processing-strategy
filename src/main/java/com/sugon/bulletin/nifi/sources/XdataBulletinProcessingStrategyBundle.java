package com.sugon.bulletin.nifi.sources;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleDetails;
import org.apache.nifi.nar.NarBundleUtil;
import org.apache.nifi.nar.NarClassLoader;
import org.apache.nifi.nar.NarUnpacker;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public final class XdataBulletinProcessingStrategyBundle {
    private static Logger logger = LoggerFactory.getLogger(org.apache.nifi.events.XdataBulletinProcessingStrategyBundle.class);
    private static final String BULLETIN_PROCESSING_STRATEGY_DIRECTORY = "nifi.bulletin.processing.strategy.directory";

    public static Set<Bundle> create(final NiFiProperties niFiProperties) throws IOException, ClassNotFoundException {
        Set<Bundle> bundles = new HashSet<>();
        final ClassLoader parentClassLoader = org.apache.nifi.events.XdataBulletinProcessingStrategyBundle.class.getClassLoader();

        final String narLibraryDirectory = niFiProperties.getProperty(BULLETIN_PROCESSING_STRATEGY_DIRECTORY);
        if (StringUtils.isBlank(narLibraryDirectory)) {
            throw new IllegalStateException("Unable to create bulletin processing strategy bundle because " + BULLETIN_PROCESSING_STRATEGY_DIRECTORY + " was null or empty");
        }
        logger.info("公告写入外部系统策略目录:{}", narLibraryDirectory);

        File narLibraryDirectoryFile = new File(narLibraryDirectory);
        File[] files = narFilesInDirectory(narLibraryDirectoryFile);
        if (files.length == 0) {
            logger.warn("未发现任何公告写入策略nar包");
        }
        for (File file : files) {
            File unpackedFile = NarUnpacker.unpackNar(file, narLibraryDirectoryFile);
            ClassLoader narClassLoader = createNarClassLoader(unpackedFile, parentClassLoader);
            BundleDetails bundleDetails = NarBundleUtil.fromNarDirectory(unpackedFile);
            Bundle bundle = new Bundle(bundleDetails, narClassLoader);
            bundles.add(bundle);
        }
        return bundles;
    }

    private static ClassLoader createNarClassLoader(final File narDirectory, final ClassLoader parentClassLoader) throws IOException, ClassNotFoundException {
        logger.debug("Loading NAR file: " + narDirectory.getAbsolutePath());
        final ClassLoader narClassLoader = new NarClassLoader(narDirectory, parentClassLoader);
        logger.info("Loaded NAR file: " + narDirectory.getAbsolutePath() + " as class loader " + narClassLoader);
        return narClassLoader;
    }

    private static File[] narFilesInDirectory(File narLibraryDirectoryFile) {
        return narLibraryDirectoryFile.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return !pathname.isDirectory() && pathname.getName().toLowerCase().endsWith(".nar");
            }
        });
    }
}
