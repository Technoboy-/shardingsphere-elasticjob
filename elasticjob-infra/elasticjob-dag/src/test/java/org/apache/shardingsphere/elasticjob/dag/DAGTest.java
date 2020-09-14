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
import org.apache.shardingsphere.elasticjob.dag.run.RuntimeJobDag;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DAGTest {
    
    private Set<String> actualJobs;
    private Set<String> expectedJobs;
    
    @Test
    public void testQueue() {
        // Test a queue: 1->2->3->4, expect [4, 3, 2, 1]
        List<String> jobs_1 = new ArrayList<>();
        jobs_1.add("4");
        jobs_1.add("3");
        jobs_1.add("2");
        jobs_1.add("1");
        RuntimeJobDag jobDag_1 = createJobDag(jobs_1);
        jobDag_1.addParentToChild("4", "3");
        jobDag_1.addParentToChild("3", "2");
        jobDag_1.addParentToChild("2", "1");
        jobDag_1.validate();
        jobDag_1.generateJobList();
        assertTrue(jobDag_1.hasNextJob());
        assertEquals(jobDag_1.getNextJob(), "4");
        jobDag_1.finishJob("4");
        assertEquals(jobDag_1.getNextJob(), "3");
        jobDag_1.finishJob("3");
        assertEquals(jobDag_1.getNextJob(), "2");
        jobDag_1.finishJob("2");
        assertEquals(jobDag_1.getNextJob(), "1");
        jobDag_1.finishJob("1");
        assertFalse(jobDag_1.hasNextJob());
    }
    
    @Test
    public void testDag() {
        DAG.Builder builder = DAG.newBuilder("test");
        builder.addJob(new DummyJob("4", ""));
        builder.addJob(new DummyJob("3", "4"));
        builder.addJob(new DummyJob("2", "3"));
        builder.addJob(new DummyJob("1", "9"));
        
        DAG dag = builder.build();
        RuntimeJobDag jobDag = new RuntimeJobDag(dag);
        for (Map.Entry<String, Job> entry : dag.getJobs().entrySet()) {
            jobDag.addNode(entry.getKey());
            Job node = entry.getValue();
            Set<String> parentIds = node.getParentIds();
            if (parentIds != null && !parentIds.isEmpty()) {
                for (String parentId : parentIds) {
                    jobDag.addParentToChild(parentId, entry.getKey());
                }
            }
        }
        jobDag.validate();
        
        List<String> jobs_1 = new ArrayList<>();
        jobs_1.add("4");
        jobs_1.add("3");
        jobs_1.add("2");
        jobs_1.add("1");
        RuntimeJobDag jobDag_1 = createJobDag(jobs_1);
        jobDag_1.addParentToChild("4", "3");
        jobDag_1.addParentToChild("3", "2");
        jobDag_1.addParentToChild("2", "1");
        jobDag_1.addParentToChild("1", "3");
//        jobDag_1.validate();
    }
    
    @Test
    public void testRegularDAGAndGenerateJobList() {
        List<String> jobs = new ArrayList<>();
        // DAG from
        // https://en.wikipedia.org/wiki/Topological_sorting#/media/File:Directed_acyclic_graph_2.svg
        jobs.add("5");
        jobs.add("11");
        jobs.add("2");
        jobs.add("7");
        jobs.add("8");
        jobs.add("9");
        jobs.add("3");
        jobs.add("10");
        RuntimeJobDag jobDag = createJobDag(jobs);
        jobDag.addParentToChild("5", "11");
        jobDag.addParentToChild("11", "2");
        jobDag.addParentToChild("7", "11");
        jobDag.addParentToChild("7", "8");
        jobDag.addParentToChild("11", "9");
        jobDag.addParentToChild("11", "10");
        jobDag.addParentToChild("8", "9");
        jobDag.addParentToChild("3", "8");
        jobDag.addParentToChild("3", "10");
        jobDag.generateJobList();
        
        testRegularDAGHelper(jobDag);
        jobDag.generateJobList();
        // Should have the original job list
        testRegularDAGHelper(jobDag);
    }
    
    private void testRegularDAGHelper(RuntimeJobDag jobDag) {
        emptyJobSets();
        // 5, 7, 3 are un-parented nodes to start with
        actualJobs.add(jobDag.getNextJob());
        actualJobs.add(jobDag.getNextJob());
        actualJobs.add(jobDag.getNextJob());
        assertFalse(jobDag.hasNextJob());
        expectedJobs.add("5");
        expectedJobs.add("7");
        expectedJobs.add("3");
        assertEquals(actualJobs, expectedJobs);
        jobDag.finishJob("3");
        
        // Once 3 finishes, ready-list should still be empty
        assertFalse(jobDag.hasNextJob());
        
        jobDag.finishJob("7");
        // Once 3 and 7 both finish, 8 should be ready
        emptyJobSets();
        actualJobs.add(jobDag.getNextJob());
        expectedJobs.add("8");
        assertEquals(actualJobs, expectedJobs);
        assertFalse(jobDag.hasNextJob());
        
        jobDag.finishJob("5");
        // Once 5 finishes, 11 should be ready
        emptyJobSets();
        expectedJobs.add(jobDag.getNextJob());
        actualJobs.add("11");
        assertEquals(actualJobs, expectedJobs);
        assertFalse(jobDag.hasNextJob());
        
        jobDag.finishJob("11");
        jobDag.finishJob("8");
        // Once 11 and 8 finish, 2, 9, 10 should be ready
        emptyJobSets();
        actualJobs.add(jobDag.getNextJob());
        actualJobs.add(jobDag.getNextJob());
        actualJobs.add(jobDag.getNextJob());
        expectedJobs.add("2");
        expectedJobs.add("10");
        expectedJobs.add("9");
        assertEquals(actualJobs, expectedJobs);
        assertFalse(jobDag.hasNextJob());
        
        // When all jobs are finished, no jobs should be left
        jobDag.finishJob("9");
        System.out.println(jobDag.getInflightJobList());
        jobDag.finishJob("10");
        jobDag.finishJob("2");
        assertFalse(jobDag.hasNextJob());
    }
    
    private void emptyJobSets() {
        actualJobs = new HashSet<>();
        expectedJobs = new HashSet<>();
    }
    
    private RuntimeJobDag createJobDag(List<String> jobs) {
        RuntimeJobDag jobDag = new RuntimeJobDag();
        for (String job : jobs) {
            jobDag.addNode(job);
        }
        return jobDag;
    }
}
