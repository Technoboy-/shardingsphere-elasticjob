package org.apache.shardingsphere.elasticjob.dag;

public interface DagListener {
    
    void onComplete(final String dagName, final DagState dagState);
    
}
