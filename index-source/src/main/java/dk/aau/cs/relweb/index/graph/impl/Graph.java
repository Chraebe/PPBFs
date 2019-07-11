package com.anonymous.index.graph.impl;

import com.anonymous.index.graph.GraphBase;
import com.anonymous.index.node.NodeBase;
import com.anonymous.index.util.Triple;
import com.google.gson.Gson;

public class Graph extends GraphBase {
    public Graph(NodeBase node, String baseUri, String id) {
        super(node, baseUri, id);
    }

    @Override
    public boolean identify(Triple triplePattern) {
        return triplePattern.getPredicate().equals("ANY")
                || triplePattern.getPredicate().startsWith("?")
                || triplePattern.getPredicate().equals(getId());
    }

    @Override
    public int hashCode() {
        return getBaseUri().hashCode() + getId().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj.getClass() != getClass()) return false;
        Graph other = (Graph) obj;
        return getBaseUri().equals(other.getBaseUri()) && getId().equals(other.getId());
    }

    @Override
    public String toString() {
        return toJSONString();
    }

    private String toJSONString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
