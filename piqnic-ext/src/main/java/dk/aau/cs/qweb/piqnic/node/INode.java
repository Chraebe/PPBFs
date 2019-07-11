package dk.aau.cs.qweb.piqnic.node;

import dk.aau.cs.qweb.piqnic.bloom.IBloomFilter;
import dk.aau.cs.qweb.piqnic.data.Dataset;
import dk.aau.cs.qweb.piqnic.data.FragmentBase;
import dk.aau.cs.qweb.piqnic.data.MetaFragmentBase;
import dk.aau.cs.qweb.piqnic.index.impl.LocationalIndex;
import dk.aau.cs.qweb.piqnic.index.impl.PpbfIndex;
import dk.aau.cs.qweb.piqnic.peer.Peer;
import dk.aau.cs.qweb.piqnic.util.Constituents;
import dk.aau.cs.qweb.piqnic.util.Triple;
import dk.aau.cs.qweb.piqnic.util.Tuple;
import org.rdfhdt.hdt.triples.TripleString;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public interface INode {
    void addDataset(Dataset dataset);

    void removeFragment(FragmentBase fragment);

    void addFragment(FragmentBase fragment);

    void addFragment(FragmentBase fragment, List<TripleString> triple);

    void addNeighbour(Peer peer);

    void addNeighbours(List<Peer> peers);

    void addFragments(List<FragmentBase> fragments) throws IOException;

    void shuffle() throws IOException;

    int getPort();

    String getIp();

    UUID getId();

    List<FragmentBase> getAllFragments();

    List<Peer> getRandomPeers(int num);

    List<Peer> getNeighbours();

    int fragmentCount();

    void removePeer(Peer peer);

    void processTriplePattern(Triple triple, PrintWriter writer);

    void processTriplePatternBound(Triple triple, List<Map<String, String>> bindings, PrintWriter writer);

    long estimateResult(Triple triple);

    boolean insertFragment(FragmentBase fragmentBase);

    List<Dataset> getDatasets();

    void getConstituents(PrintWriter writer);

    void getConstituentsForFragment(String baseUri, String id, PrintWriter writer);

    int getNumJoinable(Constituents constituents);

    List<Peer> getLeastRelated(int size);

    boolean hasFragment(FragmentBase fragment);

    List<TripleString> getTriples(FragmentBase fragmentBase);

    FragmentBase getFragment(String baseUri, String id);

    void addTriplesToFragment(FragmentBase fragment, List<TripleString> triples);

    void removeTriplesFromFragments(FragmentBase fragment, List<TripleString> triples);

    Set<MetaFragmentBase> buildLocationalIndex();

    Map<Tuple<MetaFragmentBase, MetaFragmentBase>, IBloomFilter<String>> buildLocalPpbfs();

    Set<MetaFragmentBase> getMetaFragments();

    void indexLocationalIndex();

    void indexSummaryGraph();

    LocationalIndex getLocationalIndex();

    PpbfIndex getPpbfIndex();

    List<String> processTriplePatternIndexLocally(Triple triple, List<Map<String, String>> bindings, MetaFragmentBase fragmentBase);

    void processTriplePatternIndex(Triple triple, List<Map<String, String>> bindings, MetaFragmentBase fragmentBase, PrintWriter writer);

    IBloomFilter<String> getConstituentsBloomFilter(FragmentBase fragment);
}
