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

package org.apache.shardingsphere.elasticjob.api;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * ElasticJob dag configuration.
 */
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class DagConfiguration {
    
    private final String dagName;
    
    private String cron;
    
    private final Map<JobRuntimeConfiguration, String> jobRuntimeConfigurations;
    
    /**
     * Create ElasticJob configuration builder.
     *
     * @param dagName dag name
     * @return ElasticJob configuration builder
     */
    public static Builder newBuilder(final String dagName) {
        return new Builder(dagName);
    }
    
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        
        private final String dagName;
        
        private String cron;
        
        private final Map<JobRuntimeConfiguration, String> jobConfigurationMap = new HashMap<>();
        
        public Builder cron(final String cron) {
            if (null != cron) {
                this.cron = cron;
            }
            return this;
        }
        
        public Builder addJobConfiguration(final JobRuntimeConfiguration jobRuntimeConfiguration, final String parentIdsStr) {
            this.jobConfigurationMap.put(jobRuntimeConfiguration, parentIdsStr);
            return this;
        }
        
        public final DagConfiguration build() {
            Preconditions.checkArgument(!Strings.isNullOrEmpty(dagName), "dagName can not be empty.");
            return new DagConfiguration(dagName, cron, jobConfigurationMap);
        }
    }
}
