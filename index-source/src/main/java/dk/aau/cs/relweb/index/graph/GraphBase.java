package com.anonymous.index.graph;

import com.anonymous.index.node.NodeBase;

public abstract class GraphBase implements IGraph {
    private NodeBase node;
    private String baseUri;
    private String id;

    public GraphBase(NodeBase node, String baseUri, String id) {
        this.node = node;
        this.baseUri = baseUri;
        this.id = id;
    }

    public NodeBase getNode() {
        return node;
    }

    public String getBaseUri() {
        return baseUri;
    }

    public String getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return baseUri.hashCode() + id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj.getClass() != GraphBase.class) return false;
        GraphBase other = (GraphBase) obj;
        return baseUri.equals(other.baseUri) && id.equals(other.id);
    }

    @Override
    public String toString() {
        return "GraphBase{" +
                "node=" + node +
                ", baseUri='" + baseUri + '\'' +
                ", id='" + id + '\'' +
                '}';
    }
}
