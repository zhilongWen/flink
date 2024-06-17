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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract {@link PipelineExecutor} used to execute {@link Pipeline pipelines} on dedicated
 * (per-job) clusters.
 *
 * @param <ClusterID> the type of the id of the cluster.
 * @param <ClientFactory> the type of the {@link ClusterClientFactory} used to create/retrieve a
 *     client to the target cluster.
 */
@Internal
@Deprecated
public class AbstractJobClusterExecutor<
                ClusterID, ClientFactory extends ClusterClientFactory<ClusterID>>
        implements PipelineExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJobClusterExecutor.class);

    private final ClientFactory clusterClientFactory;

    public AbstractJobClusterExecutor(@Nonnull final ClientFactory clusterClientFactory) {
        this.clusterClientFactory = checkNotNull(clusterClientFactory);
    }

    @Override
    public CompletableFuture<JobClient> execute(
            @Nonnull final Pipeline pipeline,
            @Nonnull final Configuration configuration,
            @Nonnull final ClassLoader userCodeClassloader)
            throws Exception {

        //将 StreamGraph 转成 JobGraph
        //创建、启动了 YarnClient， 包含了一些yarn、flink的配置和环境信息
        //获取 Flink 集群特有资源配置 JobManager内存、TaskManager内存、每个Tm的slot数
        //在 Yarn 上部署集群

        // 将 流图（StreamGraph） 转换成 作业图（JobGraph）
        final JobGraph jobGraph =
                PipelineExecutorUtils.getJobGraph(pipeline, configuration, userCodeClassloader);

        // 1.创建一个 YarnClient 与 Yarn ResourceManager 通信
        //2.获取 flink 集群配置信息，jobManager 内存，taskManager 内存，每个 taskMagaer 的 slot 数量等
        //3.获取启动 AM 入口类 org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint
        //4.部署前检查
        //  部署前检查 jar包路径、conf路径、yarn最大核数....
        //  检查指定的yarn队列是否存在
        //  检查yarn是否有足够的资源
        //5.初始化 hdfs 文件系统和文件上传器 YarnApplicationFileUploader（里面包含了 FS、对应的HDFS路径、Yarn 应用 id ...），上传配置及 jar 包信息
        //  上传flink conf 下的 log4j 文件
        //  上传作业的 jar 包和依赖的 jar 包
        //  上传 jobGraph，会先保存在一个本地临时文件中，上传完后将其删除
        //  上传 flink-dist jar 包
        //  上传Flink的配置文件 flink-conf.yaml
        //  ...
        //6.配置启动 applicationMaster 启动必备的环境信息，和启动 applicationMaster 的指令并配置 jobmanager 的内存信息
        //  jobmanager 内存配置
        //  封装启动 AppMaster 的脚本
        //  给 Yarn 容器设置 Flink 的运行环境及依赖信息
        //7.通过 YarnClient 提交启动 ApplicationMaster 的相关指令给 ResourceManager，ResourceManager 收到指令后选择
        //  一台空闲的 NodeManager 启动容器，并启动 ApplicationMaster
        //  执行其入口类 org.apache.flink.yarn.entrypoint.YarnJobClusterEntrypoint


        // 集群描述器：创建、启动了 YarnClient， 包含了一些yarn、flink的配置和环境信息
        try (final ClusterDescriptor<ClusterID> clusterDescriptor =
                // 创建启动 YarnClient
                clusterClientFactory.createClusterDescriptor(configuration)) {
            final ExecutionConfigAccessor configAccessor =
                    ExecutionConfigAccessor.fromConfiguration(configuration);

            // 集群特有资源配置：JobManager内存、TaskManager内存、每个Tm的slot数
            final ClusterSpecification clusterSpecification =
                    clusterClientFactory.getClusterSpecification(configuration);

            // 部署 jobGraph
            final ClusterClientProvider<ClusterID> clusterClientProvider =
                    clusterDescriptor.deployJobCluster(
                            clusterSpecification, jobGraph, configAccessor.getDetachedMode());
            LOG.info("Job has been submitted with JobID " + jobGraph.getJobID());

            return CompletableFuture.completedFuture(
                    new ClusterClientJobClientAdapter<>(
                            clusterClientProvider, jobGraph.getJobID(), userCodeClassloader));
        }
    }
}
