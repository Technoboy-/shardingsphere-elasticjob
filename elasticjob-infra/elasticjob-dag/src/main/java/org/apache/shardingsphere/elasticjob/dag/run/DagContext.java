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

import lombok.Data;
import org.apache.shardingsphere.elasticjob.dag.DagState;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Data
public final class DagContext {
    
    private DagState dagState;
    
    private final RuntimeJobDag runtimeJobDag;
    
    private final ConcurrentHashMap<String, JobState> jobStates = new ConcurrentHashMap<>();
    
    public DagContext(RuntimeJobDag runtimeJobDag) {
        this.runtimeJobDag = runtimeJobDag;
        this.dagState = DagState.INIT;
    }
    
    public Set<String> getAllNodes() {
        return runtimeJobDag.getAllNodes();
    }
    
    public JobState getJobState(final String jobId) {
        return jobStates.get(jobId);
    }
    
    public void finishJob(final String jobId, final JobState jobState) {
        runtimeJobDag.finishJob(jobId);
        jobStates.put(jobId, jobState);
        //write dag state to storage
    }
    
    public String getNextJob() {
        return runtimeJobDag.getNextJob();
    }
    
    public boolean hasNextJob() {
        return runtimeJobDag.hasNextJob();
    }
    
    public void setJobState(final String jobId, final JobState jobState) {
        jobStates.put(jobId, jobState);
    }
}
