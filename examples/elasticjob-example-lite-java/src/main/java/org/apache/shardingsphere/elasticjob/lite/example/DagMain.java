package com.immomo.ejob;

import org.apache.shardingsphere.elasticjob.api.DagConfiguration;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.api.JobRuntimeConfiguration;
import org.apache.shardingsphere.elasticjob.http.props.HttpJobProperties;
import org.apache.shardingsphere.elasticjob.lite.api.bootstrap.impl.DagJobBootstrap;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;
import org.apache.shardingsphere.elasticjob.reg.zookeeper.ZookeeperConfiguration;
import org.apache.shardingsphere.elasticjob.reg.zookeeper.ZookeeperRegistryCenter;

public class DagHttpJob {
    
    public static void main(String[] args) throws Exception {
        ZookeeperConfiguration zookeeperConfiguration = new ZookeeperConfiguration("localhost:7181", "elasticjob-dag");
        CoordinatorRegistryCenter regCenter = new ZookeeperRegistryCenter(zookeeperConfiguration);
        regCenter.init();
        //
        JobConfiguration job1 = JobConfiguration.newBuilder("httpJob1", 1).overwrite(true)
                .setProperty(HttpJobProperties.URI_KEY, "http://www.baidu.com")
                .setProperty(HttpJobProperties.METHOD_KEY, "GET")
                .shardingItemParameters("0=Beijing,1=Shanghai").build();
        JobRuntimeConfiguration jobRuntimeConfig1 = JobRuntimeConfiguration.newBuilder(job1).elasticJobType("HTTP").build();
        //
        JobConfiguration job2 = JobConfiguration.newBuilder("simpleJob", 3).overwrite(true)
                .shardingItemParameters("0=Beijing,1=Shanghai").build();
        JobRuntimeConfiguration jobRuntimeConfig2 = JobRuntimeConfiguration.newBuilder(job2).elasticJob(new JavaSimpleJob()).build();
        //
        DagConfiguration dagConfiguration = DagConfiguration.newBuilder("test-dag").cron("0/10 * * * * ?")
                .addJobConfiguration(jobRuntimeConfig1, "")
                .addJobConfiguration(jobRuntimeConfig2, "httpJob1")
                .build();
        //
        DagJobBootstrap jobBootstrap = new DagJobBootstrap(regCenter, dagConfiguration);
        jobBootstrap.schedule();
    }
}
