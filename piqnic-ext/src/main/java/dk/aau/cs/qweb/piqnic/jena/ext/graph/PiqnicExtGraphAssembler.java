package dk.aau.cs.qweb.piqnic.jena.ext.graph;

import dk.aau.cs.qweb.piqnic.jena.PiqnicJenaConstants;
import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.Mode;
import org.apache.jena.assembler.assemblers.AssemblerBase;
import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PiqnicExtGraphAssembler extends AssemblerBase implements Assembler {
    private static final Logger log = LoggerFactory.getLogger(PiqnicExtGraphAssembler.class);
    private static boolean initialized;

    public static void init() {
        if(initialized) {
            return;
        }

        initialized = true;

        Assembler.general.implementWith(PiqnicJenaConstants.PIQNIC_GRAPH, new PiqnicExtGraphAssembler());
    }

    @Override
    public Model open(Assembler a, Resource root, Mode mode)
    {
        try {
            PiqnicExtGraph graph = new PiqnicExtGraph();
            return ModelFactory.createModelForGraph(graph);
        } catch (Exception e) {
            log.error("Error creating graph: {}", e);
            throw new AssemblerException(root, "Error creating graph / "+e.toString());
        }
    }

    static {
        init();
    }
}
