package dk.aau.cs.qweb.piqnic.jena.ext;

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

import java.util.NoSuchElementException;

public class PiqnicExtJenaConstants {
    private static final String BASE_URI = "http://qweb.cs.aau.dk/piqnic/";
    public static final Resource PIQNIC_GRAPH = ResourceFactory.createResource(BASE_URI+"fuseki#PIQNICGraph") ;
    public static long NTB = 0;
    public static int NM = 0;

    public final static int BIND_NUM = 1000;
    public static IndexType INDEX = IndexType.LOC;

    public enum IndexType {
        LOC, SUM;

        public static IndexType fromString(String str) {
            if(str.equals("locational") || str.equals("LOC"))
                return LOC;
            if(str.equals("summarygraph") || str.equals("SUM"))
                return SUM;
            throw new NoSuchElementException();
        }
    }
}
