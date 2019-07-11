package com.anonymous.index.index.impl;

import com.anonymous.index.graph.IGraph;
import com.anonymous.index.index.IndexBase;
import com.anonymous.index.index.IndexMapping;
import com.anonymous.index.ppbf.IBloomFilter;
import com.anonymous.index.util.Triple;
import com.anonymous.index.util.Tuple;

import java.util.*;

public class PpbfIndex extends IndexBase {
    private Map<Tuple<IGraph, IGraph>, IBloomFilter<String>> blooms = new HashMap<>();
    private Map<IGraph, IBloomFilter<String>> bs = new HashMap<>();
    private Set<IGraph> fragments = new HashSet<>();


    @Override
    public boolean isBuilt() {
        return bs.size() > 0;
    }

    private String bound(Triple t1, Triple t2) {
        if (t1.getSubject().equals(t2.getSubject()) || t1.getSubject().equals(t2.getObject())) return t1.getSubject();
        else if (t1.getObject().equals(t2.getSubject()) || t1.getObject().equals(t2.getObject())) return t1.getObject();
        return null;
    }

    private boolean isVar(String e) {
        return e.equals("ANY") || e.startsWith("?");
    }

    @Override
    public IndexMapping getMapping(List<Triple> query) {
        Map<Triple, Set<IGraph>> identifiable = new HashMap<>();
        for (Triple t : query) {
            identifiable.put(t, new HashSet<>());
        }

        String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        for (IGraph f : fragments) {
            for (Triple t : query) {
                if (f.identify(t)) {
                    if (t.getSubject().matches(regex)) {
                        if (!bs.get(f).mightContain(t.getSubject())) {
                            continue;
                        }
                    }
                    if (t.getObject().matches(regex)) {
                        if (!bs.get(f).mightContain(t.getObject())) {
                            continue;
                        }
                    }

                    identifiable.get(t).add(f);
                }

            }
        }

        Map<Tuple<Triple, Triple>, String> bindings = new HashMap<>();
        for (Triple t1 : query) {
            for (Triple t2 : query) {
                if (t1.equals(t2) || bindings.containsKey(new Tuple<>(t1, t2)) || bindings.containsKey(new Tuple<>(t2, t1)))
                    continue;
                String b = bound(t1, t2);
                if (b != null && b.startsWith("?")) {
                    bindings.put(new Tuple<>(t1, t2), b);
                }
            }
        }

        boolean change = true;
        while (change) {
            change = false;

            for (Map.Entry<Tuple<Triple, Triple>, String> binding : bindings.entrySet()) {
                Triple t1 = binding.getKey().getFirst();
                Triple t2 = binding.getKey().getSecond();
                String b = binding.getValue();

                Set<IGraph> f1s = identifiable.get(t1);
                Set<IGraph> f2s = identifiable.get(t2);

                Set<IGraph> good1 = new HashSet<>(f1s.size());
                Set<IGraph> good2 = new HashSet<>(f2s.size());

                for (IGraph f1 : f1s) {
                    for (IGraph f2 : f2s) {
                        if (f1.equals(f2)) continue;
                        Tuple<IGraph, IGraph> tuple = new Tuple<>(f1, f2);
                        if (!blooms.containsKey(tuple)) tuple = new Tuple<>(f2, f1);
                        if (blooms.containsKey(tuple)) {
                            IBloomFilter<String> intersection = blooms.get(tuple);
                            if (!intersection.isEmpty()) {
                                good1.add(f1);
                                good2.add(f2);
                            }
                        }
                    }
                }

                if (good1.size() < f1s.size()) {
                    change = true;
                    identifiable.put(t1, good1);
                }
                if (good2.size() < f2s.size()) {
                    change = true;
                    identifiable.put(t2, good2);
                }
            }
        }

        return new IndexMapping(identifiable);
    }

    @Override
    public String toString() {
        return "PpbfIndex{" +
                "bs=" + bs +
                '}';
    }
}
