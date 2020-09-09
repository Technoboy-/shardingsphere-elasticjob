package org.apache.shardingsphere.elasticjob.dag;

import org.apache.shardingsphere.elasticjob.dag.run.JobContext;

public interface JobListener {
    
    void beforeExecute(final JobContext jobContext);
    
    void onComplete(final JobContext jobContext);
}
