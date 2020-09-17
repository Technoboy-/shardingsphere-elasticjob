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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.shardingsphere.elasticjob.dag.run;

import org.apache.shardingsphere.elasticjob.dag.DAG;
import org.apache.shardingsphere.elasticjob.dag.Job;
import org.apache.shardingsphere.elasticjob.dag.storage.DagStorage;
import org.apache.shardingsphere.elasticjob.dag.storage.zk.ZookeeperConfiguration;
import org.apache.shardingsphere.elasticjob.dag.storage.zk.ZookeeperDagStorage;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class DagRunner {
    
    private final ZookeeperConfiguration zkConfig = new ZookeeperConfiguration("localhost:7181", "dag");
    
    private DagStorage dagStorage = new ZookeeperDagStorage(zkConfig);
    
    private JobRegistry jobRegistry = new JobRegistry();
    
    private DagDispatcher dagDispatcher = new DagDispatcher(dagStorage, jobRegistry);
    
    private final RuntimeJobDag runtimeJobDag;
    
    public DagRunner(final DAG dag) {
        this.runtimeJobDag = getRuntimeJobDag(dag);
        registerJobs(jobRegistry, runtimeJobDag);
    }
    
    public void run() {
        dagDispatcher.dispatch(runtimeJobDag);
    }
    
    public void stop() {
        dagDispatcher.close();
    }
    
    private RuntimeJobDag getRuntimeJobDag(final DAG dag) {
        Objects.requireNonNull(dag);
        Objects.requireNonNull(dag.getName(), "dagName is empty");
        RuntimeJobDag jobDag = new RuntimeJobDag(dag);
        for (Map.Entry<String, Job> entry : dag.getJobs().entrySet()) {
            jobDag.addNode(entry.getKey());
            Job job = entry.getValue();
            Set<String> parentIds = job.getParentIds();
            if (parentIds != null && !parentIds.isEmpty()) {
                for (String parentId : parentIds) {
                    jobDag.addParentToChild(parentId, entry.getKey());
                }
            }
        }
        jobDag.validate();
        return jobDag;
    }
    
    private void registerJobs(final JobRegistry jobRegistry, final RuntimeJobDag runtimeJobDag) {
        for (String jobId : runtimeJobDag.getAllNodes()) {
            DAG dag = runtimeJobDag.getDag();
            Job job = dag.getJob(jobId);
            if (null == job) {
                throw new IllegalStateException(String.format("job %s not registered", jobId));
            }
            jobRegistry.registry(job);
        }
    }
}
