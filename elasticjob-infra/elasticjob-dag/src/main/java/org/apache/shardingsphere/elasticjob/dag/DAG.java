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

package org.apache.shardingsphere.elasticjob.dag;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class DAG {
    
    private final String name;
    
    private final Map<String, Job> jobs;
    
    private final Set<DagListener> listeners;
    
    public Job getJob(String jobId) {
        return jobs.get(jobId);
    }
    
    public static Builder newBuilder(final String name) {
        return new Builder(name);
    }
    
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        
        private final String name;
        
        private final Map<String, Job> jobs = new HashMap<>();
        
        private final Set<DagListener> listeners = new HashSet<>();
        
        public Builder addJob(final Job job) {
            jobs.put(job.getId(), job);
            return this;
        }
        
        public Builder addListener(final DagListener dagListener) {
            listeners.add(dagListener);
            return this;
        }
        
        public final DAG build() {
            Objects.requireNonNull(name, "name can not be empty.");
            return new DAG(name, jobs, listeners);
        }
    }
}
