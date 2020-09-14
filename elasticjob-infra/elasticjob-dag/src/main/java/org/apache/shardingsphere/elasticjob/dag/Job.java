/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.shardingsphere.elasticjob.dag;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public abstract class Job {
    
    @JsonIgnore
    private final static ObjectMapper MAPPER = new ObjectMapper();
    
    @Getter
    private final String id;
    
    @Getter
    private final Set<String> parentIds = new HashSet<>();
    
    @Getter
    private final Set<JobListener> listeners = new HashSet<>();
    
    private final long defaultTimeoutMillis = Long.MAX_VALUE;
    
    private long timeout = defaultTimeoutMillis;
    
    public Job(final String id) {
        this(id, "");
    }
    
    public Job(final String id, final String parentIdsStr) {
        this.id = id;
        setParentIdsStr(parentIdsStr);
    }
    
    public void setParentIdsStr(final String parentIdsStr) {
        if (parentIdsStr != null && !parentIdsStr.trim().isEmpty()) {
            String tmp[] = parentIdsStr.split(",");
            parentIds.addAll(Arrays.asList(tmp));
        }
    }
    
    public static Job fromJson(final String json) throws Exception {
        return MAPPER.readValue(json, Job.class);
    }
    
    public String toJson() throws Exception {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }
    
    public Job addListener(final JobListener jobListener) {
        listeners.add(jobListener);
        return this;
    }
    
    public long getTimeout() {
        return timeout;
    }
    
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
    
    public abstract void execute();
}
