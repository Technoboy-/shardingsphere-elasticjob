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

import org.apache.shardingsphere.elasticjob.dag.fixture.DummyJob;
import org.apache.shardingsphere.elasticjob.dag.run.DagRunner;
import org.apache.shardingsphere.elasticjob.dag.run.JobContext;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DagRunnerTest {
    
    
    @Test(expected = IllegalStateException.class)
    public void assertRunWithIllegalStateException() {
        Dag dag = new Dag();
        dag.addJob(new DummyJob("4", ""));
        dag.addJob(new DummyJob("3", "4"));
        dag.addJob(new DummyJob("2", "3"));
        dag.addJob(new DummyJob("1", "9"));
        DagRunner runner = new DagRunner();
        runner.run(dag);
    }
    
    @Test
    public void assertRun() {
        DagListener dagListener = new DagListener() {
            @Override
            public void onComplete(final String dagName, final DagState dagState) {
                System.out.println(String.format("dag %s complete with : %s", dagName, dagState));
            }
        };
        Dag dag = new Dag();
        dag.setName("test");
        dag.addListener(dagListener);
        JobListener jobListener = new JobListener() {
            @Override
            public void beforeExecute(JobContext jobContext) {
                System.out.println(String.format("before execute id :%s", jobContext.getId()));
            }
            
            @Override
            public void onComplete(JobContext jobContext) {
                System.out.println("onComplete jobContext " + jobContext);
            }
        };
        DummyJob dummyJob4 = new DummyJob("4");
        dummyJob4.addListener(jobListener);
        dag.addJob(dummyJob4);
        dag.addJob(new DummyJob("3", "4"));
        dag.addJob(new DummyJob("2", "3"));
        dag.addJob(new DummyJob("1"));
        dag.addJob(new DummyJob("9"));
        
        DagRunner runner = new DagRunner();
        runner.run(dag);
        CountDownLatch latch = new CountDownLatch(1);
        try {
            latch.await(5, TimeUnit.SECONDS);
            runner.stop();
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
