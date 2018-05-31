package ares.remoting.framework.zookeeper;


import ares.remoting.framework.helper.IPHelper;
import ares.remoting.framework.helper.PropertyConfigeHelper;
import ares.remoting.framework.model.InvokerService;
import ares.remoting.framework.model.ProviderService;
import com.alibaba.fastjson.JSON;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;

/**
 * 注册中心实现
 *
 * @author liyebing created on 17/2/12.
 * @version $Id$
 */
public class RegisterCenter implements IRegisterCenter4Invoker, IRegisterCenter4Provider, IRegisterCenter4Governance {

    private static RegisterCenter registerCenter = new RegisterCenter();

    //服务提供者列表,Key:服务提供者接口  value:服务提供者服务方法列表
    /** 存放服务+服务提供者原信息，可能被多个服务者共同注册 */
    private static final Map<String, List<ProviderService>> providerServiceMap = Maps.newConcurrentMap();

    //服务端ZK服务元信息,选择服务(第一次直接从ZK拉取,后续由ZK的监听机制主动更新)
    private static final Map<String, List<ProviderService>> serviceMetaDataMap4Consume = com.google.common.collect.Maps.newConcurrentMap();

    /** 以下内容是Zookeeper根据properties配置文件生成的 */
    private static String ZK_SERVICE = PropertyConfigeHelper.getZkService();
    private static int ZK_SESSION_TIME_OUT = PropertyConfigeHelper.getZkConnectionTimeout();
    private static int ZK_CONNECTION_TIME_OUT = PropertyConfigeHelper.getZkConnectionTimeout();
    private static String ROOT_PATH = "/config_register";
    public static String PROVIDER_TYPE = "provider";
    public static String INVOKER_TYPE = "consumer";

    /** 具体Zookeeper操作的客户端 */
    private static volatile ZkClient zkClient = null;

    /**
     * Zookeeper包装类构造函数。
     */
    private RegisterCenter() {
    }

    public static RegisterCenter singleton() {
        return registerCenter;
    }

    /**
     * 注册生产者是被具体某个服务提供方在spring初始化的时候调用的。
     *
     * @param serviceMetaData       某个服务提供方所提供的服务元信息列表
     */
    @Override
    public void registerProvider(final List<ProviderService> serviceMetaData) {
        if (CollectionUtils.isEmpty(serviceMetaData)) {
            return;
        }

        //连接zk,注册服务
        /**
         * 特别注意，这个`synchronized`有三重作用：
         * 1、保证`Zookeeper`只被实例化一次；
         * 2、保证`providerServiceMap`是被互斥访问的；
         * 3、看似保证`Zookeeper`上服务路径结点只有集群中第一台机器来创建（Zookeeper有分布式锁，即便同时来注册，也只有一台机器能首先注册成功，其余均失败）
         */
        synchronized (RegisterCenter.class) {

            // 遍历这个服务提供方提供的所有服务
            for (ProviderService provider : serviceMetaData) {

                // 接口名
                String serviceItfKey = provider.getServiceItf().getName();
                // 静态缓存中已有的服务
                List<ProviderService> providers = providerServiceMap.get(serviceItfKey);

                // 第一次注册服务就新建List把自己放入
                if (providers == null) {
                    providers = Lists.newArrayList();
                }
                providers.add(provider);

                // 更新一把(对象改变了也可以重新放入的)
                providerServiceMap.put(serviceItfKey, providers);
            }

            if (zkClient == null) {
                zkClient = new ZkClient(ZK_SERVICE, ZK_SESSION_TIME_OUT, ZK_CONNECTION_TIME_OUT, new SerializableSerializer());
            }

            //创建 ZK命名空间/当前部署应用APP命名空间/
            String APP_KEY = serviceMetaData.get(0).getAppKey();
            String ZK_PATH = ROOT_PATH + "/" + APP_KEY;
            // 路径`Zookeeper根路径+应用名`是集群中第一台机器来注册吗？
            boolean exist = zkClient.exists(ZK_PATH);
            if (!exist) {
                // 是集群中第一台服务提供者机器，就创建一个永久应用名结点
                zkClient.createPersistent(ZK_PATH, true);
            }

            // 循环遍历最新的服务Map
            for (Map.Entry<String, List<ProviderService>> entry : providerServiceMap.entrySet()) {
                //服务分组
                String groupName = entry.getValue().get(0).getGroupName();
                //创建服务提供者
                // `serviceNode`是服务接口名
                String serviceNode = entry.getKey();
                // Zookeeper中全路径：`Zookeeper根路径`+分组名+服务名+`消费者/生产者`
                String servicePath = ZK_PATH + "/" + groupName + "/" + serviceNode + "/" + PROVIDER_TYPE;

                exist = zkClient.exists(servicePath);
                if (!exist) {
                    // 是集群中第一台注册这个分组的这个服务的，就创建永久服务结点
                    zkClient.createPersistent(servicePath, true);
                }

                //创建当前服务器节点
                int serverPort = entry.getValue().get(0).getServerPort();//服务端口
                int weight = entry.getValue().get(0).getWeight();//服务权重
                int workerThreads = entry.getValue().get(0).getWorkerThreads();//服务工作线程

                // Zookeeper服务器所在的机器IP
                String localIp = IPHelper.localIp();

                /**
                 * 服务IP路径：
                 * `servicePath`是：`Zookeeper根路径`+分组名+服务名+`消费者/生产者`；
                 * `currentServiceIpNode`是`servicePath`+`Zookeeper所在IP地址`+`服务端口号`+`服务权重`+`工作线程`+`服务分组名`
                 */
                String currentServiceIpNode = servicePath + "/" + localIp + "|" + serverPort + "|" + weight + "|" + workerThreads + "|" + groupName;
                exist = zkClient.exists(currentServiceIpNode);
                if (!exist) {
                    //注意,这里创建的是临时节点
                    zkClient.createEphemeral(currentServiceIpNode);
                }

                //监听注册服务的变化,同时更新数据到本地缓存
                // 在这个服务节点上注册监听器(不同服务提供方可能会引起`服务名+分组名`结点的变化)
                zkClient.subscribeChildChanges(servicePath, new IZkChildListener() {
                    @Override
                    public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                        if (currentChilds == null) {
                            currentChilds = Lists.newArrayList();
                        }

                        //存活的服务IP列表
                        List<String> activityServiceIpList = Lists.newArrayList(Lists.transform(currentChilds, new Function<String, String>() {
                            @Override
                            public String apply(String input) {
                                return StringUtils.split(input, "|")[0];
                            }
                        }));
                        refreshActivityService(activityServiceIpList);
                    }
                });

            }
        }
    }

    @Override
    public Map<String, List<ProviderService>> getProviderServiceMap() {
        return providerServiceMap;
    }

    /**
     * 在服务调用方spring的bean工厂中代理生成的时候调用，将Zookeeper中已注册的服务信息放入到`serviceMetaDataMap4Consume`中。
     *
     * PS:服务调用者触发是否有点不合理？理论上应该由服务中心自己维护与刷新。
     *
     * @param remoteAppKey
     * @param groupName
     */
    @Override
    public void initProviderMap(String remoteAppKey, String groupName) {
        if (MapUtils.isEmpty(serviceMetaDataMap4Consume)) {
            serviceMetaDataMap4Consume.putAll(fetchOrUpdateServiceMetaData(remoteAppKey, groupName));
        }
    }

    @Override
    public Map<String, List<ProviderService>> getServiceMetaDataMap4Consume() {
        return serviceMetaDataMap4Consume;
    }

    /**
     * 每当一个服务消费者spring起来后，向服务中心注册自己所调用服务，在`在Zookeeper服务中心`中建立消费者节点。
     *
     * PS：没有看到有监听消费者结点变更的处理，这个也应该由服务中心自己维护。
     *
     * @param invoker
     */
    @Override
    public void registerInvoker(InvokerService invoker) {
        if (invoker == null) {
            return;
        }

        //连接zk,注册服务
        synchronized (RegisterCenter.class) {

            if (zkClient == null) {
                zkClient = new ZkClient(ZK_SERVICE, ZK_SESSION_TIME_OUT, ZK_CONNECTION_TIME_OUT, new SerializableSerializer());
            }
            //创建 ZK命名空间/当前部署应用APP命名空间/
            boolean exist = zkClient.exists(ROOT_PATH);
            if (!exist) {
                zkClient.createPersistent(ROOT_PATH, true);
            }

            //创建服务消费者节点
            String remoteAppKey = invoker.getRemoteAppKey();
            String groupName = invoker.getGroupName();
            String serviceNode = invoker.getServiceItf().getName();
            String servicePath = ROOT_PATH + "/" + remoteAppKey + "/" + groupName + "/" + serviceNode + "/" + INVOKER_TYPE;
            exist = zkClient.exists(servicePath);
            if (!exist) {
                zkClient.createPersistent(servicePath, true);
            }

            //创建当前服务器节点
            String localIp = IPHelper.localIp();
            String currentServiceIpNode = servicePath + "/" + localIp;
            exist = zkClient.exists(currentServiceIpNode);
            if (!exist) {
                //注意,这里创建的是临时节点
                zkClient.createEphemeral(currentServiceIpNode);
            }
        }
    }

    /**
     * 利用ZK的自动刷新机制监听服务名分组名中存活服务提供者列表数据。
     *
     * @param serviceIpList     异步监听传入的变化
     */
    private void refreshActivityService(List<String> serviceIpList) {
        if (serviceIpList == null) {
            serviceIpList = Lists.newArrayList();
        }

        Map<String, List<ProviderService>> currentServiceMetaDataMap = Maps.newHashMap();
        for (Map.Entry<String, List<ProviderService>> entry : providerServiceMap.entrySet()) {
            String key = entry.getKey();
            List<ProviderService> providerServices = entry.getValue();

            List<ProviderService> serviceMetaDataModelList = currentServiceMetaDataMap.get(key);
            if (serviceMetaDataModelList == null) {
                serviceMetaDataModelList = Lists.newArrayList();
            }

            for (ProviderService serviceMetaData : providerServices) {
                if (serviceIpList.contains(serviceMetaData.getServerIp())) {
                    serviceMetaDataModelList.add(serviceMetaData);
                }
            }
            currentServiceMetaDataMap.put(key, serviceMetaDataModelList);
        }
        providerServiceMap.clear();
        System.out.println("currentServiceMetaDataMap,"+ JSON.toJSONString(currentServiceMetaDataMap));
        providerServiceMap.putAll(currentServiceMetaDataMap);
    }

    /**
     * 监听Zookeeper的子节点变化，来刷新服务原信息映射。
     *
     * @param serviceIpList
     */
    private void refreshServiceMetaDataMap(List<String> serviceIpList) {
        if (serviceIpList == null) {
            serviceIpList = Lists.newArrayList();
        }

        // 最新服务提供列表
        Map<String, List<ProviderService>> currentServiceMetaDataMap = Maps.newHashMap();

        // 遍历已经存在的服务
        for (Map.Entry<String, List<ProviderService>> entry : serviceMetaDataMap4Consume.entrySet()) {

            // 服务接口名
            String serviceItfKey = entry.getKey();
            // 服务提供列表
            List<ProviderService> serviceList = entry.getValue();

            // 如果第一次遍历到、则新建服务List放入，后续遍历到则拿出存在的List列表
            List<ProviderService> providerServiceList = currentServiceMetaDataMap.get(serviceItfKey);
            if (providerServiceList == null) {
                providerServiceList = Lists.newArrayList();
            }

            // 如果原来的服务现在还在最新的IP列表里，则放入服务List中，否则抛弃
            for (ProviderService serviceMetaData : serviceList) {
                if (serviceIpList.contains(serviceMetaData.getServerIp())) {
                    providerServiceList.add(serviceMetaData);
                }
            }

            // 刷新这个接口名下的服务提供列表
            currentServiceMetaDataMap.put(serviceItfKey, providerServiceList);
        }

        // 清空服务提供Map，并且重新放入
        serviceMetaDataMap4Consume.clear();
        serviceMetaDataMap4Consume.putAll(currentServiceMetaDataMap);
    }


    /**
     * 服务调用消费方从服务中心中获取已经存在的、或更新监听变更后的服务数据。
     *
     * @param remoteAppKey
     * @param groupName
     * @return
     */
    private Map<String, List<ProviderService>> fetchOrUpdateServiceMetaData(String remoteAppKey, String groupName) {
        // 服务名称作为key、服务提供方列表作为值
        final Map<String, List<ProviderService>> providerServiceMap = Maps.newConcurrentMap();
        //连接zk
        synchronized (RegisterCenter.class) {
            if (zkClient == null) {
                zkClient = new ZkClient(ZK_SERVICE, ZK_SESSION_TIME_OUT, ZK_CONNECTION_TIME_OUT, new SerializableSerializer());
            }
        }

        //从ZK获取服务提供者列表：
        // 服务注册结点(根路径+app应用名+分组名)，获取节点下的所有列表(children)
        // 第一层路径是：根路径+应用名+分组名
        String providePath = ROOT_PATH + "/" + remoteAppKey + "/" + groupName;
        List<String> providerServices = zkClient.getChildren(providePath);

        for (String serviceName : providerServices) {

            // 第二层路径是：第一层路径+服务名+服务提供者标识
            String servicePath = providePath + "/" + serviceName + "/" + PROVIDER_TYPE;
            List<String> ipPathList = zkClient.getChildren(servicePath);

            // 对获取到的服务提供方信息列表进行处理
            for (String ipPath : ipPathList) {
                // 路径信息格式：IP地址、端口号、权重、工作线程数、分组
                String serverIp = StringUtils.split(ipPath, "|")[0];
                String serverPort = StringUtils.split(ipPath, "|")[1];
                int weight = Integer.parseInt(StringUtils.split(ipPath, "|")[2]);
                int workerThreads = Integer.parseInt(StringUtils.split(ipPath, "|")[3]);
                String group = StringUtils.split(ipPath, "|")[4];

                List<ProviderService> providerServiceList = providerServiceMap.get(serviceName);
                if (providerServiceList == null) {
                    providerServiceList = Lists.newArrayList();
                }

                // 服务调用者信息
                ProviderService providerService = new ProviderService();
                // 设置服务接口
                try {
                    providerService.setServiceItf(ClassUtils.getClass(serviceName));
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
                // 设置服务属性
                providerService.setServerIp(serverIp);
                providerService.setServerPort(Integer.parseInt(serverPort));
                providerService.setWeight(weight);
                providerService.setWorkerThreads(workerThreads);
                providerService.setGroupName(group);
                providerServiceList.add(providerService);

                // 按照服务名放入服务提供者列表
                providerServiceMap.put(serviceName, providerServiceList);
            }

            //监听注册服务的变化,同时更新数据到本地缓存
            zkClient.subscribeChildChanges(servicePath, new IZkChildListener() {
                @Override
                public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                    if (currentChilds == null) {
                        currentChilds = Lists.newArrayList();
                    }
                    currentChilds = Lists.newArrayList(Lists.transform(currentChilds, new Function<String, String>() {
                        @Override
                        public String apply(String input) {
                            return StringUtils.split(input, "|")[0];
                        }
                    }));
                    refreshServiceMetaDataMap(currentChilds);
                }
            });
        }
        return providerServiceMap;
    }

    /**
     * 服务注册中心【服务治理(可视化)专用】：查询当前服务的发布方和调用方都有哪些机器。
     *
     * @param serviceName
     * @param appKey
     * @return
     */
    @Override
    public Pair<List<ProviderService>, List<InvokerService>> queryProvidersAndInvokers(String serviceName, String appKey) {
        //服务消费者列表
        List<InvokerService> invokerServices = Lists.newArrayList();
        //服务提供者列表
        List<ProviderService> providerServices = Lists.newArrayList();

        //连接zk
        if (zkClient == null) {
            synchronized (RegisterCenter.class) {
                if (zkClient == null) {
                    zkClient = new ZkClient(ZK_SERVICE, ZK_SESSION_TIME_OUT, ZK_CONNECTION_TIME_OUT, new SerializableSerializer());
                }
            }
        }

        String parentPath = ROOT_PATH + "/" + appKey;
        //获取 ROOT_PATH + APP_KEY注册中心子目录列表
        List<String> groupServiceList = zkClient.getChildren(parentPath);
        if (CollectionUtils.isEmpty(groupServiceList)) {
            return Pair.of(providerServices, invokerServices);
        }

        for (String group : groupServiceList) {
            String groupPath = parentPath + "/" + group;
            //获取ROOT_PATH + APP_KEY + group 注册中心子目录列表
            List<String> serviceList = zkClient.getChildren(groupPath);
            if (CollectionUtils.isEmpty(serviceList)) {
                continue;
            }
            for (String service : serviceList) {
                //获取ROOT_PATH + APP_KEY + group +service 注册中心子目录列表
                String servicePath = groupPath + "/" + service;
                List<String> serviceTypes = zkClient.getChildren(servicePath);
                if (CollectionUtils.isEmpty(serviceTypes)) {
                    continue;
                }
                for (String serviceType : serviceTypes) {
                    if (StringUtils.equals(serviceType, PROVIDER_TYPE)) {
                        //获取ROOT_PATH + APP_KEY + group +service+serviceType 注册中心子目录列表
                        String providerPath = servicePath + "/" + serviceType;
                        List<String> providers = zkClient.getChildren(providerPath);
                        if (CollectionUtils.isEmpty(providers)) {
                            continue;
                        }

                        //获取服务提供者信息
                        for (String provider : providers) {
                            String[] providerNodeArr = StringUtils.split(provider, "|");

                            ProviderService providerService = new ProviderService();
                            providerService.setAppKey(appKey);
                            providerService.setGroupName(group);
                            providerService.setServerIp(providerNodeArr[0]);
                            providerService.setServerPort(Integer.parseInt(providerNodeArr[1]));
                            providerService.setWeight(Integer.parseInt(providerNodeArr[2]));
                            providerService.setWorkerThreads(Integer.parseInt(providerNodeArr[3]));
                            providerServices.add(providerService);
                        }

                    } else if (StringUtils.equals(serviceType, INVOKER_TYPE)) {
                        //获取ROOT_PATH + APP_KEY + group +service+serviceType 注册中心子目录列表
                        String invokerPath = servicePath + "/" + serviceType;
                        List<String> invokers = zkClient.getChildren(invokerPath);
                        if (CollectionUtils.isEmpty(invokers)) {
                            continue;
                        }

                        //获取服务消费者信息
                        for (String invoker : invokers) {
                            InvokerService invokerService = new InvokerService();
                            invokerService.setRemoteAppKey(appKey);
                            invokerService.setGroupName(group);
                            invokerService.setInvokerIp(invoker);
                            invokerServices.add(invokerService);
                        }
                    }
                }
            }

        }
        return Pair.of(providerServices, invokerServices);
    }

}
