/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.dispatcher.DispatcherOperationCaches;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.HistoryServerArchivist;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServices;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunner;
import org.apache.flink.runtime.dispatcher.runner.DispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmanager.HaServicesJobPersistenceComponentFactory;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerServiceImpl;
import org.apache.flink.runtime.rest.JobRestEndpointFactory;
import org.apache.flink.runtime.rest.RestEndpointFactory;
import org.apache.flink.runtime.rest.SessionRestEndpointFactory;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcherImpl;
import org.apache.flink.runtime.rest.handler.legacy.metrics.VoidMetricFetcher;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.runtime.webmonitor.WebMonitorEndpoint;
import org.apache.flink.runtime.webmonitor.retriever.LeaderGatewayRetriever;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceRetriever;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcGatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract class which implements the creation of the {@link DispatcherResourceManagerComponent}
 * components.
 */
public class DefaultDispatcherResourceManagerComponentFactory
        implements DispatcherResourceManagerComponentFactory {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Nonnull private final DispatcherRunnerFactory dispatcherRunnerFactory;

    @Nonnull private final ResourceManagerFactory<?> resourceManagerFactory;

    @Nonnull private final RestEndpointFactory<?> restEndpointFactory;

    public DefaultDispatcherResourceManagerComponentFactory(
            @Nonnull DispatcherRunnerFactory dispatcherRunnerFactory,
            @Nonnull ResourceManagerFactory<?> resourceManagerFactory,
            @Nonnull RestEndpointFactory<?> restEndpointFactory) {
        this.dispatcherRunnerFactory = dispatcherRunnerFactory;
        this.resourceManagerFactory = resourceManagerFactory;
        this.restEndpointFactory = restEndpointFactory;
    }

    @Override
    public DispatcherResourceManagerComponent create(
            Configuration configuration,
            ResourceID resourceId,
            Executor ioExecutor,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            BlobServer blobServer,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            MetricRegistry metricRegistry,
            ExecutionGraphInfoStore executionGraphInfoStore,
            MetricQueryServiceRetriever metricQueryServiceRetriever,
            Collection<FailureEnricher> failureEnrichers,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {

        LeaderRetrievalService dispatcherLeaderRetrievalService = null;
        LeaderRetrievalService resourceManagerRetrievalService = null;
        WebMonitorEndpoint<?> webMonitorEndpoint = null;
        ResourceManagerService resourceManagerService = null;
        DispatcherRunner dispatcherRunner = null;

        try {
            dispatcherLeaderRetrievalService =
                    highAvailabilityServices.getDispatcherLeaderRetriever();

            resourceManagerRetrievalService =
                    highAvailabilityServices.getResourceManagerLeaderRetriever();

            final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever =
                    new RpcGatewayRetriever<>(
                            rpcService,
                            DispatcherGateway.class,
                            DispatcherId::fromUuid,
                            new ExponentialBackoffRetryStrategy(
                                    12, Duration.ofMillis(10), Duration.ofMillis(50)));

            final LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever =
                    new RpcGatewayRetriever<>(
                            rpcService,
                            ResourceManagerGateway.class,
                            ResourceManagerId::fromUuid,
                            new ExponentialBackoffRetryStrategy(
                                    12, Duration.ofMillis(10), Duration.ofMillis(50)));

            final ScheduledExecutorService executor =
                    WebMonitorEndpoint.createExecutorService(
                            configuration.get(RestOptions.SERVER_NUM_THREADS),
                            configuration.get(RestOptions.SERVER_THREAD_PRIORITY),
                            "DispatcherRestEndpoint");

            final long updateInterval =
                    configuration.get(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL);
            final MetricFetcher metricFetcher =
                    updateInterval == 0
                            ? VoidMetricFetcher.INSTANCE
                            : MetricFetcherImpl.fromConfiguration(
                                    configuration,
                                    metricQueryServiceRetriever,
                                    dispatcherGatewayRetriever,
                                    executor);

            //  创建 WebMonitorEndpoint 并启动
            //  在 Standalone模式下：DispatcherRestEndpoint
            // 1、restEndpointFactory = SessionRestEndpointFactory
            // 2、webMonitorEndpoint = DispatcherRestEndpoint
            // 3、highAvailabilityServices.getClusterRestEndpointLeaderElectionService() = ZooKeeperLeaderElectionService
            // 初始化各种 Handler，包括： JobSubmitHandler
            //不管是这里初始化的那个 Handler， 里面都有一个 handleRequest 的方法
            //ChannelInboundHandler  channelRead0()  方法，这个方法会自动被 Netty 去调用执行
            //channelRead0() 的底层，最终调用的就是 Hnadler.handleRequest() 方法
            //创建并启动 Netty 服务端
            //选举并启动 WebMonitorEndpoint
            //通过 ZooKeeperLeaderElectionHaServices 进行选举，最终调用的是 ZooKeeper 的 API 框架 Curator 实现的
            //选举成功的执行 isLeader() 再调用 grantLeadership()  最终将 leader 节点的信息写入 zookeeper 的 leaderPath 路径下
            //选举失败的执行 notLeader()
            //确认 leader 节点将并将 leader 节点的信息写入 zookeeper 的 leaderPath 路径下
            webMonitorEndpoint =
                    restEndpointFactory.createRestEndpoint(
                            configuration,
                            dispatcherGatewayRetriever,
                            resourceManagerGatewayRetriever,
                            blobServer,
                            executor,
                            metricFetcher,
                            // org.apache.flink.runtime.leaderelection.DefaultLeaderElection -》 ZooKeeperLeaderElectionHaServices
                            highAvailabilityServices.getClusterRestEndpointLeaderElection(),
                            fatalErrorHandler);

            log.debug("Starting Dispatcher REST endpoint.");
            webMonitorEndpoint.start();

            final String hostname = RpcUtils.getHostname(rpcService);


            //ResourceManager 是一个 RpcEndpoint 创建成功后首先会调用 onStart()，启动成功后给自己发送 START 消息，表示  ResourceManager 已经成功启动好了
            //创建 ResourceManager 的过程中会调用其父类 RpcEndpoint 启动 RPC 服务，RpcEndpoint 的 akka rpc，与初始化服务的 rpc 创建方式一样
            // 【old  服务是通过 JDK 提供的动态代理 Proxy InvocationHandler 创建的，与高可用和和心跳服务等 rpc 服务通过 SupervisorActor 包装 actor 的方式不同 】，
            // ResourceManager 创建完成后会立即调用 onStart 方法
            //如果是 yarn 模式会启动 yarn 的 resourceManager，并通过 yarnclient 注册 ApplicationMaster，如果是 Standalone 模式则不会做任何处理，
            //再然后通过 ZooKeeperLeaderElectionService 进行 leader 的选举工作
            //选举成功的执行 isLeader() 再调用 grantLeadership()  最终将 leader 节点的信息写入 zookeeper 的 leaderPath 路径下
            //选举失败的执行 notLeader()
            //ResourceManager 的 leader 节点选举出来后会在其内部启动 SlotManager 用于资源管理，并启动两个定时任务用户管理 ResourceManager 与 taskManager 和 jobManager（jobMaster）的心跳
            //所有动作完成后会给 ResourceManager 自己发送一个 START 信息，表示集群以启动
            resourceManagerService =
                    ResourceManagerServiceImpl.create(
                            resourceManagerFactory,
                            configuration,
                            resourceId,
                            rpcService,
                            highAvailabilityServices,
                            heartbeatServices,
                            delegationTokenManager,
                            fatalErrorHandler,
                            new ClusterInformation(hostname, blobServer.getPort()),
                            webMonitorEndpoint.getRestBaseUrl(),
                            metricRegistry,
                            hostname,
                            ioExecutor);

            final HistoryServerArchivist historyServerArchivist =
                    HistoryServerArchivist.createHistoryServerArchivist(
                            configuration, webMonitorEndpoint, ioExecutor);

            final DispatcherOperationCaches dispatcherOperationCaches =
                    new DispatcherOperationCaches(
                            configuration.get(RestOptions.ASYNC_OPERATION_STORE_DURATION));

            final PartialDispatcherServices partialDispatcherServices =
                    new PartialDispatcherServices(
                            configuration,
                            highAvailabilityServices,
                            resourceManagerGatewayRetriever,
                            blobServer,
                            heartbeatServices,
                            () ->
                                    JobManagerMetricGroup.createJobManagerMetricGroup(
                                            metricRegistry, hostname),
                            executionGraphInfoStore,
                            fatalErrorHandler,
                            historyServerArchivist,
                            metricRegistry.getMetricQueryServiceGatewayRpcAddress(),
                            ioExecutor,
                            dispatcherOperationCaches,
                            failureEnrichers);

            // Dispatcher 同样是一个 RpcEndpoint ，创建过程中会启动一个 rpc 服务，在创建完成后会立即启动 onStart()
            //在创建 Dispatcher 前会先创建一个 DispatcherRunner，然后通过 ZooKeeperLeaderElectionService 进行选举，DispatcherRunner 的 leader 确认后会通过 dispatcherFactory 创建 Dispatcher，创建完成后调用其 onStart 方法启动 DispatcherRunner
            //然后 DispatcherRunner 会引导程序完成初始化，首先恢复 JobGraghs，再恢复执行待恢复的 Job，调用 runJob 运行一个任务
            //再提交运行作业前会先创建一个 JobManagerRunner，并且在创建 JobManagerRunner 的同时会启动 JobMaster，因 JobMaster 也是 Dispatcher 负责启动的，JobMaster 也是一个 FencedRpcEndpoint，所有创建完后会调用其 onStart 方法
            //最后通过 JobManagerRunner 提交代恢复的作业，并在这个过程中通过调用 JobMaster 的 start 方法启动 JobMaster
            log.debug("Starting Dispatcher.");
            dispatcherRunner =
                    dispatcherRunnerFactory.createDispatcherRunner(
                            highAvailabilityServices.getDispatcherLeaderElection(),
                            fatalErrorHandler,
                            new HaServicesJobPersistenceComponentFactory(highAvailabilityServices),
                            ioExecutor,
                            rpcService,
                            partialDispatcherServices);

            log.debug("Starting ResourceManagerService.");
            //所有动作完成后会给 ResourceManager 自己发送一个 START 信息，表示集群以启动
            resourceManagerService.start();

            resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);
            dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);

            return new DispatcherResourceManagerComponent(
                    dispatcherRunner,
                    resourceManagerService,
                    dispatcherLeaderRetrievalService,
                    resourceManagerRetrievalService,
                    webMonitorEndpoint,
                    fatalErrorHandler,
                    dispatcherOperationCaches);

        } catch (Exception exception) {
            // clean up all started components
            if (dispatcherLeaderRetrievalService != null) {
                try {
                    dispatcherLeaderRetrievalService.stop();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            if (resourceManagerRetrievalService != null) {
                try {
                    resourceManagerRetrievalService.stop();
                } catch (Exception e) {
                    exception = ExceptionUtils.firstOrSuppressed(e, exception);
                }
            }

            final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

            if (webMonitorEndpoint != null) {
                terminationFutures.add(webMonitorEndpoint.closeAsync());
            }

            if (resourceManagerService != null) {
                terminationFutures.add(resourceManagerService.closeAsync());
            }

            if (dispatcherRunner != null) {
                terminationFutures.add(dispatcherRunner.closeAsync());
            }

            final FutureUtils.ConjunctFuture<Void> terminationFuture =
                    FutureUtils.completeAll(terminationFutures);

            try {
                terminationFuture.get();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            throw new FlinkException(
                    "Could not create the DispatcherResourceManagerComponent.", exception);
        }
    }

    public static DefaultDispatcherResourceManagerComponentFactory createSessionComponentFactory(
            ResourceManagerFactory<?> resourceManagerFactory) {
        return new DefaultDispatcherResourceManagerComponentFactory(
                DefaultDispatcherRunnerFactory.createSessionRunner(
                        SessionDispatcherFactory.INSTANCE),
                resourceManagerFactory,
                SessionRestEndpointFactory.INSTANCE);
    }

    public static DefaultDispatcherResourceManagerComponentFactory createJobComponentFactory(
            ResourceManagerFactory<?> resourceManagerFactory, JobGraphRetriever jobGraphRetriever) {
        return new DefaultDispatcherResourceManagerComponentFactory(
                DefaultDispatcherRunnerFactory.createJobRunner(jobGraphRetriever),
                resourceManagerFactory,
                JobRestEndpointFactory.INSTANCE);
    }
}
