package org.apache.shardingsphere.elasticjob.dag;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@Data
@RequiredArgsConstructor
public abstract class Job {
    
    @JsonIgnore
    private final static ObjectMapper MAPPER = new ObjectMapper();
    
    private String id;
    
    private Set<String> parentIds;
    
    public Job(final String id, final String parentIdsStr) {
        this.setId(id);
        if (parentIdsStr != null && !parentIdsStr.trim().isEmpty()) {
            String tmp[] = parentIdsStr.split(",");
            parentIds = new HashSet<>();
            parentIds.addAll(Arrays.asList(tmp));
        }
        this.setParentIds(parentIds);
    }
    
    public static Job fromJson(final String json) throws Exception {
        return MAPPER.readValue(json, Job.class);
    }
    
    public String toJson() throws Exception {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }
    
    
    public abstract void execute();
}
