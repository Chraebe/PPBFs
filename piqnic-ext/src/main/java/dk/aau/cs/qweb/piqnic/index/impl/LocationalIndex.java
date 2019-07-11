package dk.aau.cs.qweb.piqnic.index.impl;

import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.data.MetaFragmentBase;
import dk.aau.cs.qweb.piqnic.index.IndexMapping;
import dk.aau.cs.qweb.piqnic.index.PiqnicIndexBase;
import dk.aau.cs.qweb.piqnic.util.Triple;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class LocationalIndex extends PiqnicIndexBase {
    private Set<MetaFragmentBase> fragments = new HashSet<>();

    @Override
    public void build() {
        fragments = PiqnicClient.nodeInstance.buildLocationalIndex();
    }

    @Override
    public boolean isBuilt() {
        return fragments.size() > 0;
    }

    @Override
    public IndexMapping getMapping(List<Triple> query) {
        IndexMapping mapping = new IndexMapping();
        for(Triple triple : query) {
            for (MetaFragmentBase m : fragments) {
                if(m.identify(triple)) mapping.addMapping(triple, m);
            }
        }
        return mapping;
    }

    @Override
    public String toString() {
        return "LocationalIndex{" +
                "fragments=" + fragments +
                '}';
    }
}
