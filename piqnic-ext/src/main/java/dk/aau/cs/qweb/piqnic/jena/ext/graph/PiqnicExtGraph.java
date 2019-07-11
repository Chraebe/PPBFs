package dk.aau.cs.qweb.piqnic.jena.ext.graph;

import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.config.Configuration;
import dk.aau.cs.qweb.piqnic.data.MetaFragmentBase;
import dk.aau.cs.qweb.piqnic.jena.PiqnicJenaConstants;
import dk.aau.cs.qweb.piqnic.jena.bind.PiqnicBindings;
import dk.aau.cs.qweb.piqnic.jena.ext.iter.PiqnicExtJenaIterator;
import dk.aau.cs.qweb.piqnic.jena.ext.solver.OpExecutorPiqnicExt;
import dk.aau.cs.qweb.piqnic.jena.ext.solver.ReorderTransformationPiqnicExt;
import dk.aau.cs.qweb.piqnic.jena.solver.PiqnicEngine;
import dk.aau.cs.qweb.piqnic.jena.solver.PiqnicJenaFloodIterator;
import dk.aau.cs.qweb.piqnic.node.PiqnicNode;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.peer.IndexPeer;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Capabilities;
import org.apache.jena.graph.GraphStatisticsHandler;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.query.ARQ;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;

public class PiqnicExtGraph extends GraphBase {
    private static final Logger log = LoggerFactory.getLogger(PiqnicExtGraph.class);
    private PiqnicExtStatistics statistics = new PiqnicExtStatistics(this);
    private static PiqnicExtCapabilities capabilities = new PiqnicExtCapabilities();
    private ReorderTransformation reorderTransform;
    private PiqnicExtJenaIterator iterator = PiqnicExtJenaIterator.emptyIterator();

    static {
        QC.setFactory(ARQ.getContext(), OpExecutorPiqnicExt.opExecFactoryPiqnicExt);
        PiqnicEngine.register();
    }

    public PiqnicExtGraph() {
        reorderTransform = new ReorderTransformationPiqnicExt(this);
    }

    @Override
    protected ExtendedIterator<Triple> graphBaseFind(Triple jenaTriple) {
        return PiqnicJenaFloodIterator.emptyIterator();
    }

    public ExtendedIterator<Pair<Triple, Binding>> graphBaseFind(Triple jenaTriple, PiqnicBindings bindings, Set<MetaFragmentBase> fragments) {
        iterator = PiqnicExtJenaIterator.emptyIterator();
        List<Map<String, String>> bs = new ArrayList<>();
        List<String> strs = new ArrayList<>();
        int size = bindings.size();
        for(int i = 0; i < size; i++) {
            Binding binding = bindings.get(i);
            Iterator<Var> it = binding.vars();
            String str = "";

            Map<String, String> bb = new HashMap<>();
            while(it.hasNext()) {
                Var v = it.next();
                String var = v.asNode().toString();
                String b = binding.get(v).toString();
                bb.put(var, b);
                str = str.concat(var + "=" + b + ";;");
            }
            if(bb.size() > 0) bs.add(bb);
            if(str.length() > 2)
                str = str.substring(0, str.length()-2);

            if(str.length() > 1)
                strs.add(str);
        }

        Map<IPeer, Set<MetaFragmentBase>> fragmentMap = new HashMap<>();
        for(MetaFragmentBase f : fragments) {
            if(!fragmentMap.containsKey(f.getNode())) {
                Set<MetaFragmentBase> set = new HashSet<>();
                set.add(f);
                fragmentMap.put(f.getNode(), set);
            } else {
                fragmentMap.get(f.getNode()).add(f);
            }
        }

        List<TriplePatternProcessorThread> threads = new ArrayList<>();
        for(Map.Entry<IPeer, Set<MetaFragmentBase>> entry : fragmentMap.entrySet()) {
            TriplePatternProcessorThread t = new TriplePatternProcessorThread(new dk.aau.cs.qweb.piqnic.util.Triple(jenaTriple.getSubject().toString(), jenaTriple.getPredicate().toString(), jenaTriple.getObject().toString()),
                    bs, strs, entry.getValue(), entry.getKey());
            t.start();
            threads.add(t);
        }

        while(isRunning(threads)) ;
        return iterator;
    }

    private boolean isRunning(List<TriplePatternProcessorThread> threads) {
        for (TriplePatternProcessorThread t : threads) {
            if (t.isRunning()) return true;
        }
        return false;
    }

    private class TriplePatternProcessorThread extends Thread {
        private final dk.aau.cs.qweb.piqnic.util.Triple triple;
        private final Set<MetaFragmentBase> fragments;
        private final List<Map<String, String>> bindings;
        private final List<String> bindingStrs;
        private boolean isRunning = true;
        private IPeer peer;

        public TriplePatternProcessorThread(dk.aau.cs.qweb.piqnic.util.Triple t, List<Map<String, String>> b, List<String> bs, Set<MetaFragmentBase> f, IPeer peer) {
            this.triple = t;
            this.bindings = b;
            this.fragments = f;
            this.bindingStrs = bs;
            this.peer = peer;
        }

        boolean isRunning() {
            return isRunning;
        }

        @Override
        public void run() {
            isRunning = true;

            if(peer.equals(new IndexPeer((PiqnicNode)PiqnicClient.nodeInstance))) {
                // Process locally
                for(MetaFragmentBase fragment : fragments) {
                    iterator.addLocals(PiqnicClient.nodeInstance.processTriplePatternIndexLocally(triple, bindings, fragment));
                }
            } else {
                // Process remotely
                Socket socket;
                PrintWriter out;
                BufferedReader in;
                try {
                    socket = new Socket(peer.getAddress(), peer.getPort());
                    out = new PrintWriter(socket.getOutputStream(), true);
                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                } catch (IOException | NullPointerException e) {
                    return;
                }

                out.println("C");

                for(MetaFragmentBase fragment : fragments) {
                    out.println(fragment.getBaseUri() + ";;" + fragment.getId());
                }

                out.println("EOF");
                out.println(UUID.randomUUID() + ";"
                        + triple.getSubject() + ";"
                        + triple.getPredicate() + ";"
                        + triple.getObject());

                int size = bindingStrs.size();
                for(int i = 0; i < size; i++) {
                    String str = bindingStrs.get(i);
                    if(str.length() > 1)
                        out.println(str);
                }
                out.println("EOF");
                //PiqnicJenaConstants.NM++;

                iterator.addReader(in);
            }

            isRunning = false;
        }
    }

    @Override
    public GraphStatisticsHandler getStatisticsHandler() {
        return statistics;
    }

    @Override
    public Capabilities getCapabilities() {
        return capabilities;
    }

    @Override
    protected int graphBaseSize() {
        //return (int)statistics.getStatistic(Node.ANY, Node.ANY, Node.ANY);
        return 1000000000;
    }

    public ReorderTransformation getReorderTransform() {
        return reorderTransform;
    }

    @Override
    public void close() {
        super.close();
    }
}