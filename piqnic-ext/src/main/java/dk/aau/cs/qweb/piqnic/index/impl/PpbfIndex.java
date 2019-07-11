package dk.aau.cs.qweb.piqnic.index.impl;

import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.bloom.IBloomFilter;
import dk.aau.cs.qweb.piqnic.config.Configuration;
import dk.aau.cs.qweb.piqnic.data.MetaFragmentBase;
import dk.aau.cs.qweb.piqnic.index.IndexMapping;
import dk.aau.cs.qweb.piqnic.index.PiqnicIndexBase;
import dk.aau.cs.qweb.piqnic.node.PiqnicNode;
import dk.aau.cs.qweb.piqnic.peer.IndexPeer;
import dk.aau.cs.qweb.piqnic.peer.Peer;
import dk.aau.cs.qweb.piqnic.util.Triple;
import dk.aau.cs.qweb.piqnic.util.Tuple;

import java.io.*;
import java.util.*;

public class PpbfIndex extends PiqnicIndexBase {
    private Map<Tuple<MetaFragmentBase, MetaFragmentBase>, IBloomFilter<String>> blooms = new HashMap<>();
    private Map<MetaFragmentBase, IBloomFilter<String>> bs = new HashMap<>();
    private Set<MetaFragmentBase> fragments = new HashSet<>();

    @Override
    public void build() {
        System.out.println("Finding reachable fragments");
        Map<IndexPeer, Set<MetaFragmentBase>> map = new HashMap<>();
        try {
            map = new Peer((PiqnicNode) PiqnicClient.nodeInstance).getMetaFragmentsOrdered(Configuration.instance.getTimeToLive(), UUID.randomUUID());
        } catch (IOException e) {
            return;
        }

        System.out.println("Downloading Constituents");
        System.out.println("Found " + map.size() + " fragments");

        int i = 0;
        for (Map.Entry<IndexPeer, Set<MetaFragmentBase>> e : map.entrySet()) {
            System.out.println(i + " of " + map.size() + ": Downloading " + e.getValue().size() + " filters from " + e.getKey().getPort());
            i++;
            try {
                bs.putAll(e.getKey().getBloomsForFragments(e.getValue()));
            } catch (IOException e1) {
                System.out.println("Error, could not download from peer " + e.getKey().getPort());
                continue;
            }
        }

        fragments.addAll(bs.keySet());
        System.out.println("Gathered fragments and their filters");

        blooms = PiqnicClient.nodeInstance.buildLocalPpbfs();


        System.out.println("Local bloom done!");
    }

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
        System.out.println("Calculating Minimal Matching Subgraph");
        Map<Triple, Set<MetaFragmentBase>> identifiable = new HashMap<>();
        for (Triple t : query) {
            identifiable.put(t, new HashSet<>());
        }

        String regex = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";
        for (MetaFragmentBase f : fragments) {
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
                if (b != null) {
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

                Set<MetaFragmentBase> f1s = identifiable.get(t1);
                Set<MetaFragmentBase> f2s = identifiable.get(t2);

                Set<MetaFragmentBase> good1 = new HashSet<>(f1s.size());
                Set<MetaFragmentBase> good2 = new HashSet<>(f2s.size());


                if (isVar(b)) {
                    for (MetaFragmentBase f1 : f1s) {
                        for (MetaFragmentBase f2 : f2s) {
                            if (f1.equals(f2)) continue;
                            Tuple<MetaFragmentBase, MetaFragmentBase> tuple = new Tuple<>(f1, f2);
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
                } else {
                    for (MetaFragmentBase f1 : f1s) {
                        for (MetaFragmentBase f2 : f2s) {
                            if (f1.equals(f2)) continue;
                            Tuple<MetaFragmentBase, MetaFragmentBase> tuple = new Tuple<>(f1, f2);
                            if (!blooms.containsKey(tuple)) tuple = new Tuple<>(f2, f1);
                            if (blooms.containsKey(tuple)) {
                                IBloomFilter<String> intersection = blooms.get(tuple);
                                if (!intersection.isEmpty() && intersection.mightContain(b)) {
                                    good1.add(f1);
                                    good2.add(f2);
                                }
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

        System.out.println("Mapping calculated");
        return new IndexMapping(identifiable);
    }

    @Override
    public String toString() {
        return "PpbfIndex{" +
                "bs=" + bs +
                '}';
    }
}
