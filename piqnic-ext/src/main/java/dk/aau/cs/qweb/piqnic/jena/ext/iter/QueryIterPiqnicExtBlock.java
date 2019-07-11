package dk.aau.cs.qweb.piqnic.jena.ext.iter;

import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.index.IndexMapping;
import dk.aau.cs.qweb.piqnic.jena.ext.PiqnicExtJenaConstants;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Substitute;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter1;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderLib;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderProc;
import org.apache.jena.sparql.engine.optimizer.reorder.ReorderTransformation;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.util.FmtUtils;

import java.util.ArrayList;
import java.util.List;

public class QueryIterPiqnicExtBlock extends QueryIter1 {
    private BasicPattern pattern;
    private QueryIterator output;
    private IndexMapping mapping;

    public static QueryIterator create(QueryIterator input, BasicPattern pattern, ExecutionContext execContext) {
        return new QueryIterPiqnicExtBlock(input, pattern, execContext);
    }

    private QueryIterPiqnicExtBlock(QueryIterator input, BasicPattern pattern, ExecutionContext execContext) {
        super(input, execContext);
        this.pattern = pattern;

        ReorderTransformation reorder = ReorderLib.fixed();
        if (pattern.size() >= 2 && !input.isJoinIdentity()) {
            QueryIterPeek peek = QueryIterPeek.create(input, execContext);
            input = peek;
            Binding b = peek.peek();
            BasicPattern bgp2 = Substitute.substitute(pattern, b);
            ReorderProc reorderProc = reorder.reorderIndexes(bgp2);
            pattern = reorderProc.reorder(pattern);
        }
        QueryIterator chain = input;

        List<dk.aau.cs.qweb.piqnic.util.Triple> triples = new ArrayList<>();
        for (Triple triple : pattern) {
            triples.add(new dk.aau.cs.qweb.piqnic.util.Triple(triple.getSubject().toString(), triple.getPredicate().toString(), triple.getObject().toString()));
        }
        if (PiqnicExtJenaConstants.INDEX == PiqnicExtJenaConstants.IndexType.LOC)
            mapping = PiqnicClient.nodeInstance.getLocationalIndex().getMapping(triples);
        else mapping = PiqnicClient.nodeInstance.getPpbfIndex().getMapping(triples);

        for (Triple triple : pattern) {
            chain = new QueryIterPiqnicExt(chain, triple, execContext,
                    mapping.getMapping(new dk.aau.cs.qweb.piqnic.util.Triple(triple.getSubject().toString(),
                            triple.getPredicate().toString(),
                            triple.getObject().toString())));
        }

        this.output = chain;
    }

    protected boolean hasNextBinding() {
        return this.output.hasNext();
    }

    protected Binding moveToNextBinding() {
        return this.output.nextBinding();
    }

    protected void closeSubIterator() {
        if (this.output != null) {
            this.output.close();
        }

        this.output = null;
    }

    protected void requestSubCancel() {
        if (this.output != null) {
            this.output.cancel();
        }

    }

    protected void details(IndentedWriter out, SerializationContext sCxt) {
        out.print(Lib.className(this));
        out.println();
        out.incIndent();
        FmtUtils.formatPattern(out, this.pattern, sCxt);
        out.decIndent();
    }
}
