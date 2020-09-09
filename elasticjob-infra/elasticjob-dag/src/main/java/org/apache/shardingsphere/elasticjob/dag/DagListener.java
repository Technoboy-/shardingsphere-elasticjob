package org.apache.shardingsphere.elasticjob.dag;

public interface DagListener {
    
    void onComplete(final DagState dagState);
    
}
