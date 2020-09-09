package org.apache.shardingsphere.elasticjob.dag;

public interface JobListener {
    
    void beforeExecute(final JobContext jobContext);
    
    void onComplete(final JobContext jobContext);
}
