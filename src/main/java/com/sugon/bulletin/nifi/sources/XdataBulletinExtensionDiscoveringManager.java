package com.sugon.bulletin.nifi.sources;

import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.events.BulletinProcessingStrategy;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.InstanceClassLoader;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class XdataBulletinExtensionDiscoveringManager implements ExtensionDiscoveringManager {
    private static final Logger logger = LoggerFactory.getLogger(StandardExtensionDiscoveringManager.class);

    // Maps a service definition (interface) to those classes that implement the interface
    private final Map<Class, Set<Class>> definitionMap = new HashMap<>();

    private final Map<String, List<Bundle>> classNameBundleLookup = new HashMap<>();
    private final Map<BundleCoordinate, Set<Class>> bundleCoordinateClassesLookup = new HashMap<>();
    private final Map<BundleCoordinate, Bundle> bundleCoordinateBundleLookup = new HashMap<>();
    private final Map<ClassLoader, Bundle> classLoaderBundleLookup = new HashMap<>();

    private final Map<String, InstanceClassLoader> instanceClassloaderLookup = new ConcurrentHashMap<>();

    public XdataBulletinExtensionDiscoveringManager() {
        definitionMap.put(BulletinProcessingStrategy.class, new HashSet<>());

    }

    @Override
    public Set<Bundle> getAllBundles() {
        return classNameBundleLookup.values().stream()
                .flatMap(List::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public void discoverExtensions(final Bundle systemBundle, final Set<Bundle> narBundles) {
        // load the system bundle first so that any extensions found in JARs directly in lib will be registered as
        // being from the system bundle and not from all the other NARs
        loadExtensions(systemBundle);
        bundleCoordinateBundleLookup.put(systemBundle.getBundleDetails().getCoordinate(), systemBundle);

        discoverExtensions(narBundles);
    }

    @Override
    public void discoverExtensions(final Set<Bundle> narBundles) {
        // get the current context class loader
        ClassLoader originContextClassLoader = Thread.currentThread().getContextClassLoader();
        // consider each nar class loader
        for (Bundle bundle : narBundles) {
            // Must set the context class loader to the nar classloader itself
            // so that static initialization techniques that depend on the context class loader will work properly
            final ClassLoader ncl = bundle.getClassLoader();
            Thread.currentThread().setContextClassLoader(ncl);
            loadExtensions(bundle);
            // Create a look-up from coordinate to bundle
            bundleCoordinateBundleLookup.put(bundle.getBundleDetails().getCoordinate(), bundle);
        }

        // restore the current context class loader if appropriate
        if (null != originContextClassLoader) {
            Thread.currentThread().setContextClassLoader(originContextClassLoader);
        }
    }

    /**
     * Loads extensions from the specified bundle.
     *
     * @param bundle from which to load extensions
     */
    @SuppressWarnings("unchecked")
    private void loadExtensions(final Bundle bundle) {
        for (final Map.Entry<Class, Set<Class>> entry : definitionMap.entrySet()) {
            logger.info("definition entry key : {}", entry.getKey());
            final ServiceLoader<?> serviceLoader = ServiceLoader.load(entry.getKey(), bundle.getClassLoader());
            for (final Object o : serviceLoader) {
                try {

                    // only consider extensions discovered directly in this bundle
                    boolean registerExtension = bundle.getClassLoader().equals(o.getClass().getClassLoader());

                    if (registerExtension) {
                        registerServiceClass(o.getClass(), classNameBundleLookup, bundleCoordinateClassesLookup, bundle, entry.getValue());
                    }
                } catch (Exception e) {
                    logger.warn("Failed to register extension {} due to: {}", o.getClass().getCanonicalName(), e.getMessage());
                    if (logger.isDebugEnabled()) {
                        logger.debug("", e);
                    }
                }
            }

            classLoaderBundleLookup.put(bundle.getClassLoader(), bundle);
        }
    }

    /**
     * Registers extension for the specified type from the specified Bundle.
     *
     * @param type               the extension type
     * @param classNameBundleMap mapping of classname to Bundle
     * @param bundle             the Bundle being mapped to
     * @param classes            to map to this classloader but which come from its ancestors
     */
    private void registerServiceClass(final Class<?> type,
                                      final Map<String, List<Bundle>> classNameBundleMap,
                                      final Map<BundleCoordinate, Set<Class>> bundleCoordinateClassesMap,
                                      final Bundle bundle, final Set<Class> classes) {
        final String className = type.getName();
        final BundleCoordinate bundleCoordinate = bundle.getBundleDetails().getCoordinate();

        // get the bundles that have already been registered for the class name
        final List<Bundle> registeredBundles = classNameBundleMap.computeIfAbsent(className, key -> new ArrayList<>());
        final Set<Class> bundleCoordinateClasses = bundleCoordinateClassesMap.computeIfAbsent(bundleCoordinate, key -> new HashSet<>());

        boolean alreadyRegistered = false;
        for (final Bundle registeredBundle : registeredBundles) {
            final BundleCoordinate registeredCoordinate = registeredBundle.getBundleDetails().getCoordinate();

            // if the incoming bundle has the same coordinate as one of the registered bundles then consider it already registered
            if (registeredCoordinate.equals(bundleCoordinate)) {
                alreadyRegistered = true;
                break;
            }

        }

        // if none of the above was true then register the new bundle
        if (!alreadyRegistered) {
            registeredBundles.add(bundle);
            bundleCoordinateClasses.add(type);
            classes.add(type);

        }

    }


    @Override
    public InstanceClassLoader createInstanceClassLoader(final String classType, final String instanceIdentifier, final Bundle bundle, final Set<URL> additionalUrls) {
        if (StringUtils.isEmpty(classType)) {
            throw new IllegalArgumentException("Class-Type 不能为空");
        }

        if (StringUtils.isEmpty(instanceIdentifier)) {
            throw new IllegalArgumentException("Instance Identifier 不能为空");
        }

        if (bundle == null) {
            throw new IllegalArgumentException("Bundle 不能为空");
        }

        final ClassLoader bundleClassLoader = bundle.getClassLoader();
        InstanceClassLoader instanceClassLoader = new InstanceClassLoader(instanceIdentifier, classType, Collections.emptySet(), additionalUrls, bundleClassLoader);

        instanceClassloaderLookup.put(instanceIdentifier, instanceClassLoader);
        return instanceClassLoader;
    }

    @Override
    public InstanceClassLoader getInstanceClassLoader(final String instanceIdentifier) {
        return instanceClassloaderLookup.get(instanceIdentifier);
    }

    @Override
    public InstanceClassLoader removeInstanceClassLoader(final String instanceIdentifier) {
        if (instanceIdentifier == null) {
            return null;
        }

        final InstanceClassLoader classLoader = instanceClassloaderLookup.remove(instanceIdentifier);
        closeURLClassLoader(instanceIdentifier, classLoader);
        return classLoader;
    }

    @Override
    public void closeURLClassLoader(final String instanceIdentifier, final ClassLoader classLoader) {
        if ((classLoader instanceof URLClassLoader)) {
            final URLClassLoader urlClassLoader = (URLClassLoader) classLoader;
            try {
                urlClassLoader.close();
            } catch (IOException e) {
                logger.warn("Unable to close URLClassLoader for {}", instanceIdentifier);
            }
        }
    }

    @Override
    public List<Bundle> getBundles(final String classType) {
        if (classType == null) {
            throw new IllegalArgumentException("Class type cannot be null");
        }
        final List<Bundle> bundles = classNameBundleLookup.get(classType);
        return bundles == null ? Collections.emptyList() : new ArrayList<>(bundles);
    }

    @Override
    public Bundle getBundle(final BundleCoordinate bundleCoordinate) {
        if (bundleCoordinate == null) {
            throw new IllegalArgumentException("BundleCoordinate cannot be null");
        }
        return bundleCoordinateBundleLookup.get(bundleCoordinate);
    }

    @Override
    public Set<Class> getTypes(final BundleCoordinate bundleCoordinate) {
        if (bundleCoordinate == null) {
            throw new IllegalArgumentException("BundleCoordinate cannot be null");
        }
        final Set<Class> types = bundleCoordinateClassesLookup.get(bundleCoordinate);
        return types == null ? Collections.emptySet() : Collections.unmodifiableSet(types);
    }

    @Override
    public Bundle getBundle(final ClassLoader classLoader) {
        if (classLoader == null) {
            throw new IllegalArgumentException("ClassLoader cannot be null");
        }
        return classLoaderBundleLookup.get(classLoader);
    }

    @Override
    public Set<Class> getExtensions(final Class<?> definition) {
        if (definition == null) {
            throw new IllegalArgumentException("Class cannot be null");
        }
        final Set<Class> extensions = definitionMap.get(definition);
        return (extensions == null) ? Collections.<Class>emptySet() : extensions;
    }

    @Override
    public ConfigurableComponent getTempComponent(final String classType, final BundleCoordinate bundleCoordinate) {
        return null;
    }

    private static String getClassBundleKey(final String classType, final BundleCoordinate bundleCoordinate) {
        return classType + "_" + bundleCoordinate.getCoordinate();
    }

    @Override
    public void logClassLoaderMapping() {
        StringBuilder builder = new StringBuilder();
        builder.append("Extension Type Mapping to Bundle:");

        for (final Map.Entry<Class, Set<Class>> entry : definitionMap.entrySet()) {
            builder.append("\n\t=== ")
                    .append(entry.getKey().getSimpleName())
                    .append(" Type ===");

            for (final Class type : entry.getValue()) {
                List<Bundle> bundles = classNameBundleLookup.get(type.getName());
                if (null == bundles) {
                    bundles = Collections.emptyList();
                }
                builder.append("\n\t").append(type.getName());

                for (Bundle bundle : bundles) {
                    String coordinate = bundle.getBundleDetails().getCoordinate().getCoordinate();
                    String workingDir = bundle.getBundleDetails().getWorkingDirectory().getPath();
                    builder.append("\n\t\t")
                            .append(coordinate)
                            .append(" || ")
                            .append(workingDir);
                }
            }
            builder.append("\n\t=== End ")
                    .append(entry.getKey()
                            .getSimpleName())
                    .append(" types ===");
        }
        String log = builder.toString();
        logger.info(log);
    }
}
