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

import org.apache.commons.lang3.StringUtils;
import org.apache.shardingsphere.elasticjob.dag.DagState;
import org.apache.shardingsphere.elasticjob.dag.JobRegistry;
import org.apache.shardingsphere.elasticjob.dag.storage.DagStorage;

public class DagDispatcher implements JobStateListener {
    
    private final JobQueue jobQueue;
    
    private final DagStorage dagStorage;
    
    private DagContext dagContext;
    
    private String id = String.valueOf(System.nanoTime());
    
    public DagDispatcher(final DagStorage dagStorage, final JobRegistry jobRegistry) {
        this.dagStorage = dagStorage;
        this.jobQueue = new JobQueue(jobRegistry);
    }
    
    public void dispatch(final RuntimeJobDag dag) {
        this.dagContext = new DagContext(dagStorage, dag);
        while (dagContext.hasNextJob()) {
            String nextJobId = dagContext.getNextJob();
            jobQueue.addJob(new JobExecuteContext(id, nextJobId, this));
            dagContext.setJobState(nextJobId, JobState.RUNNING);
        }
    }
    
    public void close() {
        jobQueue.close();
    }
    
    @Override
    public void onStateChange(final String jobId, final JobState jobState) {
        dagContext.finishJob(jobId, jobState);
        if (JobState.FAIL == jobState || JobState.TIMEOUT == jobState) {
            dagContext.setDagState(DagState.of(jobState));
        }
        if (dagContext.isCompleted()) {
            dagContext.complete();
        } else {
            String nextJobId = dagContext.getNextJob();
            if (StringUtils.isNotEmpty(nextJobId)) {
                jobQueue.addJob(new JobExecuteContext(id, nextJobId, this));
                dagContext.setJobState(nextJobId, JobState.RUNNING);
            } else {
                //need open a thread to monitor in flight tasks
            }
        }
    }
}
