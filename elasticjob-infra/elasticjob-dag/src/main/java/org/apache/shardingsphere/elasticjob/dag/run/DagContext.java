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

import lombok.Getter;
import org.apache.shardingsphere.elasticjob.dag.DagState;
import org.apache.shardingsphere.elasticjob.dag.storage.DagStorage;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class DagContext {
    
    @Getter
    private DagState dagState;
    
    private final RuntimeJobDag runtimeJobDag;
    
    private final DagStorage dagStorage;
    
    private final ConcurrentHashMap<String, JobState> jobStates = new ConcurrentHashMap<>();
    
    public DagContext(final DagStorage dagStorage, final RuntimeJobDag runtimeJobDag) {
        this.dagStorage = dagStorage;
        this.runtimeJobDag = runtimeJobDag;
        setDagState(DagState.INIT);
    }
    
    public Set<String> getAllNodes() {
        return runtimeJobDag.getAllNodes();
    }
    
    public JobState getJobState(final String jobId) {
        return jobStates.get(jobId);
    }
    
    public void finishJob(final String jobId, final JobState jobState) {
        runtimeJobDag.finishJob(jobId);
        setJobState(jobId, jobState);
    }
    
    public String getNextJob() {
        return runtimeJobDag.getNextJob();
    }
    
    public boolean hasNextJob() {
        return runtimeJobDag.hasNextJob();
    }
    
    public void setJobState(final String jobId, final JobState jobState) {
        dagStorage.updateJobState(jobId, jobState);
        jobStates.put(jobId, jobState);
    }
    
    public void setDagState(final DagState dagState) {
        this.dagState = dagState;
        dagStorage.updateDagState(runtimeJobDag.getDag().getName(), dagState);
    }
    
    public boolean isCompleted() {
        if (DagState.isFinished(dagState)) {
            return true;
        }
        return !(getAllNodes().stream().filter(jobId -> {
            JobState jobState = getJobState(jobId);
            return !JobState.isFinished(jobState);
        }).findAny().isPresent());
    }
    
    public void complete() {
        runtimeJobDag.getDag().getListeners().stream().forEach(listener -> listener.onComplete(dagState));
    }
}
