package com.anonymous.index.index.impl;

import com.anonymous.index.graph.IGraph;
import com.anonymous.index.index.IndexBase;
import com.anonymous.index.index.IndexMapping;
import com.anonymous.index.util.Triple;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LocationalIndex extends IndexBase {
    private Set<IGraph> graphs = new HashSet<>();

    public void addGraph(IGraph graph) {
        graphs.add(graph);
    }

    @Override
    public boolean isBuilt() {
        return graphs.size() > 0;
    }

    @Override
    public IndexMapping getMapping(List<Triple> query) {
        IndexMapping mapping = new IndexMapping();
        for(Triple triple : query) {
            for (IGraph m : graphs) {
                if(m.identify(triple)) mapping.addMapping(triple, m);
            }
        }
        return mapping;
    }

    @Override
    public String toString() {
        return "LocationalIndex{" +
                "graphs=" + graphs +
                '}';
    }
}
