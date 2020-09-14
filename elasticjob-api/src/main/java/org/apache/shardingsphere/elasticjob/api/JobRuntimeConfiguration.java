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

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Objects;

@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class JobRuntimeConfiguration {
    
    private final JobConfiguration jobConfiguration;
    
    private final ElasticJob elasticJob;
    
    private final String elasticJobType;
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobRuntimeConfiguration that = (JobRuntimeConfiguration) o;
        return Objects.equals(getJobConfiguration().getJobName(), that.getJobConfiguration().getJobName());
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(getJobConfiguration().getJobName());
    }
    
    /**
     * Create ElasticJob configuration builder.
     *
     * @param jobConfiguration job configuration
     * @return ElasticJob configuration builder
     */
    public static Builder newBuilder(final JobConfiguration jobConfiguration) {
        return new Builder(jobConfiguration);
    }
    
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        
        private final JobConfiguration jobConfiguration;
        
        private ElasticJob elasticJob;
        
        private String elasticJobType;
        
        public Builder elasticJob(final ElasticJob elasticJob) {
            this.elasticJob = elasticJob;
            return this;
        }
        
        public Builder elasticJobType(final String elasticJobType) {
            this.elasticJobType = elasticJobType;
            return this;
        }
        
        public final JobRuntimeConfiguration build() {
            return new JobRuntimeConfiguration(jobConfiguration, elasticJob, elasticJobType);
        }
    }
    
}
