package org.apache.shardingsphere.elasticjob.dag;

public interface DagStateListener {
    
    void onComplete(final DagState dagState);
    
}
