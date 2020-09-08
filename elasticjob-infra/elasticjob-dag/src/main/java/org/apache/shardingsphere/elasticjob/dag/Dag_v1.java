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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class Dag_v1 {
    
    private final static ObjectMapper MAPPER = new ObjectMapper();
    
    private String name;
    
    private final Map<String, Node> nodes = new HashMap<>();
    
    public void addNode(Node node) {
        getNodes().put(node.getId(), node);
    }
    
    public Node getNode(String id) {
        return getNodes().get(id);
    }
    
    public Set<String> getNodeIds() {
        return getNodes().keySet();
    }
    
    public static Dag_v1 fromJson(String json) throws Exception {
        return MAPPER.readValue(json, Dag_v1.class);
    }
    
    public String toJson() throws Exception {
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }
    
    public Map<String, Node> getNodes() {
        return nodes;
    }
    
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }
    
    public static class Node {
        
        private String id;
        
        private Set<String> parentIds;
        
        public Node(String id, Set<String> parentIds) {
            this.setId(id);
            this.setParentIds(parentIds);
        }
        
        public Node(String id, String parentIdsStr) {
            this.setId(id);
            if (parentIdsStr != null && !parentIdsStr.trim().isEmpty()) {
                String tmp[] = parentIdsStr.split(",");
                parentIds = new HashSet<>();
                parentIds.addAll(Arrays.asList(tmp));
            }
            this.setParentIds(parentIds);
        }
        
        public Node() {
            setId("");
            setParentIds(new HashSet<>());
        }
        
        public String getId() {
            return id;
        }
        
        public Set<String> getParentIds() {
            return parentIds;
        }
        
        public static Node fromJson(String json) throws Exception {
            return MAPPER.readValue(json, Node.class);
        }
        
        public String toJson() throws Exception {
            return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        }
        
        public void setId(String id) {
            this.id = id;
        }
        
        public void setParentIds(Set<String> parentIds) {
            this.parentIds = parentIds;
        }
    }
}
