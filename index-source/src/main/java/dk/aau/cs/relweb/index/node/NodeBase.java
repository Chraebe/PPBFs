package com.anonymous.index.node;

import com.anonymous.index.graph.IGraph;

import java.util.*;

public abstract class NodeBase {
    protected final UUID id;
    protected final String ip;
    protected final int port;
    protected List<IGraph> graphs = new ArrayList<>();

    public NodeBase(UUID id, String ip, int port) {
        this.id = id;
        this.ip = ip;
        this.port = port;
    }

    public UUID getId() {
        return id;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public List<IGraph> getGraphs() {
        return graphs;
    }

    public void addGraph(IGraph graph) {
        this.graphs.add(graph);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeBase nodeBase = (NodeBase) o;
        return port == nodeBase.port &&
                Objects.equals(id, nodeBase.id) &&
                Objects.equals(ip, nodeBase.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ip, port);
    }
}
