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

import org.apache.shardingsphere.elasticjob.dag.JobRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;

public final class JobQueue {
    
    private final int workerSize = 10;
    
    private final BlockingQueue<JobContext> queue = new SynchronousQueue(true);
    
    private List<Worker> workerList;
    
    public JobQueue(final JobRegistry jobRegistry) {
        this.workerList = new ArrayList<>(workerList);
        for (int i = 1; i <= workerSize; i++) {
            this.workerList.add(new Worker(jobRegistry, queue));
        }
        workerList.stream().forEach(worker -> worker.start());
    }
    
    public void addJob(final JobContext jobContext) {
        try {
            queue.put(jobContext);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
