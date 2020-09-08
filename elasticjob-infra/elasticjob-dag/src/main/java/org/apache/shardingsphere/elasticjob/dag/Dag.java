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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class Dag {
    
    private final static ObjectMapper MAPPER = new ObjectMapper();
    
    private String name;
    
    private final Map<String, Job> jobs = new HashMap<>();
    
    public void addJob(final Job job) {
        getJobs().put(job.getId(), job);
    }
    
    public Job getJob(final String id) {
        return getJobs().get(id);
    }
    
    public Set<String> getJobIds() {
        return getJobs().keySet();
    }
    
    public static Dag fromJson(String json) throws Exception {
        return MAPPER.readValue(json, Dag.class);
    }
    
    public String toJson() throws Exception {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }
    
    public Map<String, Job> getJobs() {
        return jobs;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(final String name) {
        this.name = name;
    }
    
}
