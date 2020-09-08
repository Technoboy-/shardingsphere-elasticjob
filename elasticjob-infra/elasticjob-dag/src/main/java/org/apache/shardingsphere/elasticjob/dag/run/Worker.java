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

import org.apache.shardingsphere.elasticjob.dag.Job;
import org.apache.shardingsphere.elasticjob.dag.JobRegistry;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;

public final class Worker extends Thread {
    
    private final BlockingQueue<JobContext> jobQueue;
    
    private final JobRegistry jobRegistry;
    
    public Worker(final JobRegistry jobRegistry, final BlockingQueue jobQueue) {
        this.jobRegistry = jobRegistry;
        this.jobQueue = jobQueue;
    }
    
    public void start() {
        super.start();
    }
    
    @Override
    public void run() {
        try {
            JobContext jobContext = jobQueue.take();
            Optional<Job> actualJob = jobRegistry.lookup(jobContext.getJobId());
            if (actualJob.isPresent()) {
                long start = System.currentTimeMillis();
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> actualJob.get().execute());
                future.whenComplete((result, cause) -> {
                    JobState jobState = JobState.SUCCESS;
                    if (null != cause) {
                        jobState = JobState.FAIL;
                    } else if (System.currentTimeMillis() - start >= 5000) {
                        jobState = JobState.TIMEOUT;
                    }
                    jobContext.getJobStateListener().onStateChange(new JobStateContext(jobContext.getJobId(), jobState));
                });
            } else {
                throw new IllegalStateException(String.format("Job %s is not registered", jobContext.getJobId()));
            }
        } catch (InterruptedException ignore) {
            //NOP
        }
    }
}
