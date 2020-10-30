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
package org.apache.dubbo.registry.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.RegistryFactory;
import org.apache.dubbo.registry.RegistryService;
import org.apache.dubbo.registry.client.ServiceDiscovery;
import org.apache.dubbo.registry.client.ServiceDiscoveryRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.EXPORT_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.REFER_KEY;

/**
 * AbstractRegistryFactory. (SPI, Singleton, ThreadSafe) 支持spi，单例，线程安全
 *
 * @see org.apache.dubbo.registry.RegistryFactory
 */
public abstract class AbstractRegistryFactory implements RegistryFactory {

    // Log output
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRegistryFactory.class);

    // The lock for the acquisition process of the registry
    protected static final ReentrantLock LOCK = new ReentrantLock();

    // Registry Collection Map<RegistryAddress, Registry>
    protected static final Map<String, Registry> REGISTRIES = new HashMap<>();

    private static final AtomicBoolean destroyed = new AtomicBoolean(false);

    /**
     * Get all registries
     *
     * @return all registries
     */
    public static Collection<Registry> getRegistries() {
        return Collections.unmodifiableCollection(new LinkedList<>(REGISTRIES.values()));
    }

    public static Registry getRegistry(String key) {
        return REGISTRIES.get(key);
    }

    /**
     * 筛选所有服务发现的注册器
     * 获取所有的 ServiceDiscovery 对象
     * @return
     */
    public static List<ServiceDiscovery> getServiceDiscoveries() {
        return AbstractRegistryFactory.getRegistries()
                .stream()
                .filter(registry -> registry instanceof ServiceDiscoveryRegistry)
                .map(registry -> (ServiceDiscoveryRegistry) registry)
                .map(ServiceDiscoveryRegistry::getServiceDiscovery)
                .collect(Collectors.toList());
    }

    /**
     * Close all created registries
     *
     * 关闭所有已经创建的注册器
     */
    public static void destroyAll() {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Close all registries " + getRegistries());
        }
        // Lock up the registry shutdown process
        LOCK.lock();
        try {
            for (Registry registry : getRegistries()) {
                try {
                    registry.destroy();
                } catch (Throwable e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            REGISTRIES.clear();
        } finally {
            // Release the lock
            LOCK.unlock();
        }
    }

    /**
     * 工厂方法获取 Registry 的默认实现
     * @param url Registry address, is not allowed to be empty
     * @return
     */
    @Override
    public Registry getRegistry(URL url) {
        if (destroyed.get()) {
            LOGGER.warn("All registry instances have been destroyed, failed to fetch any instance. " +
                    "Usually, this means no need to try to do unnecessary redundant resource clearance, all registries has been taken care of.");
            return DEFAULT_NOP_REGISTRY;
        }

        url = URLBuilder.from(url)
                .setPath(RegistryService.class.getName())
                .addParameter(INTERFACE_KEY, RegistryService.class.getName())
                .removeParameters(EXPORT_KEY, REFER_KEY)
                .build();
        String key = createRegistryCacheKey(url);
        // Lock the registry access process to ensure a single instance of the registry
        // 锁住注册器访问的过程确保注册器只被实例化一次,单例
        LOCK.lock();
        try {
            Registry registry = REGISTRIES.get(key);
            //返回已经缓存的注册器
            if (registry != null) {
                return registry;
            }
            //create registry by spi/ioc
            //通过spi/ioc创建注册器
            registry = createRegistry(url);
            //如果创建完还是空，那么久抛异常
            if (registry == null) {
                throw new IllegalStateException("Can not create registry " + url);
            }
            //将创建好的注册器放入缓存
            REGISTRIES.put(key, registry);
            return registry;
        } finally {
            // Release the lock
            LOCK.unlock();
        }
    }

    /**
     * Create the key for the registries cache.
     * This method may be override by the sub-class.
     *
     * 创建注册器的缓存键（key）
     * 此方法可以由子类覆盖
     *
     * @param url the registration {@link URL url}
     * @return non-null
     */
    protected String createRegistryCacheKey(URL url) {
        return url.toServiceStringWithoutResolving();
    }

    protected abstract Registry createRegistry(URL url);

    /**
     * 没有找到注册器的时候，会有一个默认的注册器实现，防止空指针异常
     * 将会不注册到任何的注册中心
     */
    private static Registry DEFAULT_NOP_REGISTRY = new Registry() {
        @Override
        public URL getUrl() {
            return null;
        }

        @Override
        public boolean isAvailable() {
            return false;
        }

        @Override
        public void destroy() {

        }

        @Override
        public void register(URL url) {

        }

        @Override
        public void unregister(URL url) {

        }

        @Override
        public void subscribe(URL url, NotifyListener listener) {

        }

        @Override
        public void unsubscribe(URL url, NotifyListener listener) {

        }

        @Override
        public List<URL> lookup(URL url) {
            return null;
        }
    };

    /**
     * 从缓存中去除已经销毁的注册器
     * @param toRm
     */
    public static void removeDestroyedRegistry(Registry toRm) {
        LOCK.lock();
        try {
            REGISTRIES.entrySet().removeIf(entry -> entry.getValue().equals(toRm));
        } finally {
            LOCK.unlock();
        }
    }

    // for unit test
    /**
     * 注释说是测试用的，清除所有的注册器缓存
     */
    public static void clearRegistryNotDestroy() {
        REGISTRIES.clear();
    }

}
