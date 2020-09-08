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

public class DagDispatcher implements JobStateListener {
    
    private final JobQueue jobQueue;
    
    private DagContext dagContext;
    
    public DagDispatcher(final JobRegistry jobRegistry) {
        this.jobQueue = new JobQueue(jobRegistry);
    }
    
    public void dispatch(final RuntimeJobDag dag) {
        this.dagContext = new DagContext(dag);
        while (dagContext.hasNextJob()) {
            String nextJobId = dagContext.getNextJob();
            jobQueue.addJob(new JobContext(nextJobId, this));
            dagContext.setJobState(nextJobId, JobState.RUNNING);
        }
    }
    
    @Override
    public void onStateChange(final JobStateContext jobStateContext) {
        JobState jobState = jobStateContext.getJobState();
        dagContext.finishJob(jobStateContext.getJobId(), jobStateContext.getJobState());
        if (JobState.FAIL == jobState || JobState.TIMEOUT == jobState) {
            dagContext.setDagState(DagState.of(jobState));
            return;
        }
        if (isDagFinished()) {
            //mark dag state finish
        } else {
            String nextJobId = dagContext.getNextJob();
            if (StringUtils.isNotEmpty(nextJobId)) {
                jobQueue.addJob(new JobContext(nextJobId, this));
            } else {
                //need open a thread to monitor in flight tasks
            }
        }
    }
    
    protected boolean isDagFinished() {
        DagState dagState = dagContext.getDagState();
        if (JobState.FAIL.equals(dagState) || JobState.SUCCESS.equals(dagState)) {
            return true;
        }
        for (String job : dagContext.getAllNodes()) {
            JobState jobState = dagContext.getJobState(job);
            if (jobState != JobState.SUCCESS && jobState != JobState.FAIL && jobState != JobState.TIMEOUT) {
                return false;
            }
        }
        return true;
    }
}
