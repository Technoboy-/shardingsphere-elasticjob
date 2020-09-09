package org.apache.shardingsphere.elasticjob.dag;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public abstract class Job {
    
    @JsonIgnore
    private final static ObjectMapper MAPPER = new ObjectMapper();
    
    @Getter
    private final String id;
    
    @Getter
    private final Set<String> parentIds = new HashSet<>();
    
    @Getter
    private final Set<JobListener> listeners = new HashSet<>();
    
    public Job(final String id) {
        this(id, "");
    }
    
    public Job(final String id, final String parentIdsStr) {
        this.id = id;
        if (parentIdsStr != null && !parentIdsStr.trim().isEmpty()) {
            String tmp[] = parentIdsStr.split(",");
            parentIds.addAll(Arrays.asList(tmp));
        }
    }
    
    public static Job fromJson(final String json) throws Exception {
        return MAPPER.readValue(json, Job.class);
    }
    
    public String toJson() throws Exception {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }
    
    public Job addListener(final JobListener jobListener) {
        listeners.add(jobListener);
        return this;
    }
    
    public abstract void execute();
}
