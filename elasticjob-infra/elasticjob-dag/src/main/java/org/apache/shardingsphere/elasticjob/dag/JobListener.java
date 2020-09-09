package org.apache.shardingsphere.elasticjob.dag;

import org.apache.shardingsphere.elasticjob.dag.run.JobState;

public interface JobListener {
    
    void beforeExecute();
    
    void onComplete(final JobState jobState);
    
    void afterExecute();
}
