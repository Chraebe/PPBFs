package dk.aau.cs.qweb.piqnic.index;

import dk.aau.cs.qweb.piqnic.data.MetaFragmentBase;
import dk.aau.cs.qweb.piqnic.util.Triple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class IndexMapping {
    private Map<Triple, Set<MetaFragmentBase>> mapping = new HashMap<>();

    public IndexMapping(Map<Triple, Set<MetaFragmentBase>> mapping) {
        this.mapping = mapping;
    }

    public IndexMapping () {}

    public void addMapping(Triple triple, MetaFragmentBase fragment) {
        if(mapping.containsKey(triple)) {
            mapping.get(triple).add(fragment);
            return;
        }
        Set<MetaFragmentBase> set = new HashSet<>();
        set.add(fragment);
        mapping.put(triple, set);
    }

    public Set<MetaFragmentBase> getMapping(Triple triple){
        return mapping.get(triple);
    }

    @Override
    public String toString() {
        return "IndexMapping{" +
                "mapping=" + mapping +
                '}';
    }
}
