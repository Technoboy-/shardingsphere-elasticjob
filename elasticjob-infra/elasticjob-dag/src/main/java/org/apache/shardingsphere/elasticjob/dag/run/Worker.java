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

import lombok.extern.slf4j.Slf4j;
import org.apache.shardingsphere.elasticjob.dag.Job;
import org.apache.shardingsphere.elasticjob.dag.JobContext;

import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public final class Worker extends Thread {
    
    private final BlockingQueue<JobExecuteContext> jobQueue;
    
    private final JobRegistry jobRegistry;
    
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    
    public Worker(final JobRegistry jobRegistry, final BlockingQueue jobQueue) {
        this.jobRegistry = jobRegistry;
        this.jobQueue = jobQueue;
    }
    
    public void start() {
        isRunning.compareAndSet(false, true);
        super.start();
    }
    
    public void close() {
        this.interrupt();
        isRunning.compareAndSet(true, false);
    }
    
    @Override
    public void run() {
        while (isRunning.get()) {
            try {
                final JobExecuteContext jobExecuteContext = jobQueue.take();
                Optional<Job> actualJob = jobRegistry.lookup(jobExecuteContext.getJobId());
                if (actualJob.isPresent()) {
                    JobContext jobContext = new JobContext(jobExecuteContext.getId(), jobExecuteContext.getJobId());
                    long start = System.currentTimeMillis();
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        Job job = actualJob.get();
                        job.getListeners().forEach(listener -> listener.beforeExecute(jobContext));
                        job.execute();
                    });
                    future.whenComplete((result, cause) -> {
                        Job job = actualJob.get();
                        JobState jobState = JobState.SUCCESS;
                        if (null != cause) {
                            jobState = JobState.FAIL;
                        } else if (System.currentTimeMillis() - start >= job.getTimeout()) {
                            jobState = JobState.TIMEOUT;
                        }
                        jobExecuteContext.getJobStateListener().onStateChange(jobExecuteContext.getJobId(), jobState);
                        CompletableFuture.runAsync(() -> {
                            actualJob.get().getListeners().forEach(listener -> listener.onComplete(jobContext));
                        });
                    });
                } else {
                    throw new IllegalStateException(String.format("Job %s is not registered", jobExecuteContext.getJobId()));
                }
            } catch (InterruptedException ignore) {
                break;
            }
        }
        log.info("worker closed");
    }
}
