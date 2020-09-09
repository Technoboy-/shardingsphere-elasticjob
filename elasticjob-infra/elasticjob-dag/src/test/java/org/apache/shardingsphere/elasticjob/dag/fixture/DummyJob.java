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
    public void execute() {
        System.out.println(String.format("thread :%s id :%s executed", Thread.currentThread().getName(), getId()));
    }
}
