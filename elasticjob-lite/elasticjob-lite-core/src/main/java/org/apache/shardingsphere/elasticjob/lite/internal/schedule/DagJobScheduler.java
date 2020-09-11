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

package org.apache.shardingsphere.elasticjob.lite.internal.schedule;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.shardingsphere.elasticjob.api.DagConfiguration;
import org.apache.shardingsphere.elasticjob.api.ElasticJob;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.api.JobRuntimeConfiguration;
import org.apache.shardingsphere.elasticjob.api.listener.ElasticJobListener;
import org.apache.shardingsphere.elasticjob.dag.Dag;
import org.apache.shardingsphere.elasticjob.dag.run.DagRunner;
import org.apache.shardingsphere.elasticjob.executor.ElasticJobExecutor;
import org.apache.shardingsphere.elasticjob.infra.exception.JobSystemException;
import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobInstance;
import org.apache.shardingsphere.elasticjob.lite.api.listener.AbstractDistributeOnceElasticJobListener;
import org.apache.shardingsphere.elasticjob.lite.internal.guarantee.GuaranteeService;
import org.apache.shardingsphere.elasticjob.lite.internal.setup.JobClassNameProviderFactory;
import org.apache.shardingsphere.elasticjob.lite.internal.setup.SetUpFacade;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.SimpleThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Job scheduler.
 */
public final class DagJobScheduler {
    
    private static final String DAG_RUNNER_DATA_MAP_KEY = "dagRunner";
    
    private static final String DAG_DATA_MAP_KEY = "dag";
    
    private final CoordinatorRegistryCenter regCenter;
    
    @Getter
    private final DagConfiguration dagConfig;
    
    @Getter
    private final JobScheduleController jobScheduleController;
    
    private List<ElasticJobExecutor> jobExecutors = new ArrayList<>();
    
    private List<SchedulerFacade> schedulerFacades = new ArrayList<>();
    
    private final DagRunner dagRunner = new DagRunner();
    
    private final Dag dag = new Dag();
    
    public DagJobScheduler(final CoordinatorRegistryCenter regCenter, final DagConfiguration dagConfig) {
        this.regCenter = regCenter;
        this.dagConfig = dagConfig;
        dag.setName(dagConfig.getDagName());
        for (Map.Entry<JobRuntimeConfiguration, String> entry : dagConfig.getJobRuntimeConfigurations().entrySet()) {
            JobRuntimeConfiguration runtimeConfiguration = entry.getKey();
            JobConfiguration jobConfiguration = runtimeConfiguration.getJobConfiguration();
            String parentIdsStr = entry.getValue();
            String prefix = dag.getName() + "/jobs/";
            String jobName = prefix + jobConfiguration.getJobName();
            runtimeConfiguration.getJobConfiguration().setJobName(jobName);
            List<ElasticJobListener> elasticJobListeners = runtimeConfiguration.getElasticJobListeners();
            SetUpFacade setUpFacade = new SetUpFacade(regCenter, jobName, elasticJobListeners);
            SchedulerFacade schedulerFacade = new SchedulerFacade(regCenter, jobName);
            schedulerFacades.add(schedulerFacade);
            setUpInfo(setUpFacade, jobConfiguration, runtimeConfiguration.getElasticJob(), runtimeConfiguration.getElasticJobType());
            LiteJobFacade jobFacade = new LiteJobFacade(regCenter, jobConfiguration.getJobName(), elasticJobListeners, runtimeConfiguration.getTracingConfig());
            ElasticJobExecutor jobExecutor = null == runtimeConfiguration.getElasticJob() ? new ElasticJobExecutor(runtimeConfiguration.getElasticJobType(), jobConfiguration, jobFacade) : new ElasticJobExecutor(runtimeConfiguration.getElasticJob(), jobConfiguration, jobFacade);
            setGuaranteeServiceForElasticJobListeners(regCenter, elasticJobListeners);
            if (StringUtils.isNotEmpty(parentIdsStr)) {
                parentIdsStr = Joiner.on(",").join(Splitter.on(",").splitToList(parentIdsStr).stream().map(item -> prefix + item.trim()).collect(Collectors.toList()));
            }
            jobExecutor.setParentIdsStr(parentIdsStr);
            jobExecutors.add(jobExecutor);
            dag.addJob(jobExecutor);
        }
        jobScheduleController = createJobScheduleController();
    }
    
    private void setGuaranteeServiceForElasticJobListeners(final CoordinatorRegistryCenter regCenter, final List<ElasticJobListener> elasticJobListeners) {
        GuaranteeService guaranteeService = new GuaranteeService(regCenter, dagConfig.getDagName());
        for (ElasticJobListener each : elasticJobListeners) {
            if (each instanceof AbstractDistributeOnceElasticJobListener) {
                ((AbstractDistributeOnceElasticJobListener) each).setGuaranteeService(guaranteeService);
            }
        }
    }
    
    private JobScheduleController createJobScheduleController() {
        JobScheduleController result = new JobScheduleController(createScheduler(), createJobDetail(), dagConfig.getDagName());
        JobRegistry.getInstance().registerJob(dagConfig.getDagName(), result);
        return result;
    }
    
    private Scheduler createScheduler() {
        Scheduler result;
        try {
            StdSchedulerFactory factory = new StdSchedulerFactory();
            factory.initialize(getQuartzProps());
            result = factory.getScheduler();
        } catch (final SchedulerException ex) {
            throw new JobSystemException(ex);
        }
        return result;
    }
    
    private Properties getQuartzProps() {
        Properties result = new Properties();
        result.put("org.quartz.threadPool.class", SimpleThreadPool.class.getName());
        result.put("org.quartz.threadPool.threadCount", "1");
        result.put("org.quartz.scheduler.instanceName", dagConfig.getDagName());
        result.put("org.quartz.jobStore.misfireThreshold", "1");
        result.put("org.quartz.plugin.shutdownhook.class", JobShutdownHookPlugin.class.getName());
        result.put("org.quartz.plugin.shutdownhook.cleanShutdown", Boolean.TRUE.toString());
        return result;
    }
    
    private JobDetail createJobDetail() {
        JobDetail result = JobBuilder.newJob(DagJob.class).withIdentity(dagConfig.getDagName()).build();
        result.getJobDataMap().put(DAG_RUNNER_DATA_MAP_KEY, dagRunner);
        result.getJobDataMap().put(DAG_DATA_MAP_KEY, dag);
        return result;
    }
    
    private void setUpInfo(final SetUpFacade setUpFacade, final JobConfiguration jobConfig, final ElasticJob elasticJob, final String elasticJobType) {
        String jobClassName = null != elasticJobType ? elasticJobType :
                JobClassNameProviderFactory.getProvider().getJobClassName(elasticJob);
        setUpFacade.setUpJobConfiguration(jobClassName, jobConfig);
        JobScheduleController result = new JobScheduleController(createScheduler(), createJobDetail(), jobConfig.getJobName());
        JobRegistry.getInstance().registerJob(jobConfig.getJobName(), result);
        JobRegistry.getInstance().registerRegistryCenter(jobConfig.getJobName(), regCenter);
        JobRegistry.getInstance().addJobInstance(jobConfig.getJobName(), new JobInstance());
        JobRegistry.getInstance().setCurrentShardingTotalCount(jobConfig.getJobName(), jobConfig.getShardingTotalCount());
        setUpFacade.registerStartUpInfo(!jobConfig.isDisabled());
    }
    
    /**
     * Shutdown job.
     */
    public void shutdown() {
        schedulerFacades.stream().forEach(instance -> instance.shutdownInstance());
        jobExecutors.stream().forEach(executor -> executor.shutdown());
    }
}
