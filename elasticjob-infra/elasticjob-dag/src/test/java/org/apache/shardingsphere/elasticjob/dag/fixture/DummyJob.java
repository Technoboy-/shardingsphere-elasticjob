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
package org.apache.shardingsphere.elasticjob.dag.fixture;

import org.apache.shardingsphere.elasticjob.dag.Job;

public final class DummyJob extends Job {
    
    public DummyJob(String id) {
        super(id);
    }
    
    public DummyJob(String id, String parentIdsStr) {
        super(id, parentIdsStr);
    }
    
    @Override
    public void blockUntilCompleted() {
        //NOP
    }
    
    @Override
    public void execute() {
        System.out.println(String.format("thread :%s id :%s executed", Thread.currentThread().getName(), getId()));
    }
}
