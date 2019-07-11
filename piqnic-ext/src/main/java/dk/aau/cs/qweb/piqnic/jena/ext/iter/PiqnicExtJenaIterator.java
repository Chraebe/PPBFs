package dk.aau.cs.qweb.piqnic.jena.ext.iter;

import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.jena.PiqnicJenaConstants;
import dk.aau.cs.qweb.piqnic.jena.exceptions.QueryInterruptedException;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.reasoner.rulesys.Util;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingMap;
import org.apache.jena.util.iterator.NiceIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

public class PiqnicExtJenaIterator extends NiceIterator<Pair<Triple, Binding>> {
    private Queue<String> local = new LinkedList<>();
    private Queue<BufferedReader> readers = new LinkedList<>();
    private BufferedReader current = null;
    private String nextLine;
    private boolean empty = false;
    private Set<String> seen = new HashSet<>();

    private PiqnicExtJenaIterator() {
        empty = true;
    }

    public void addReader(BufferedReader reader) {
        empty = false;
        readers.add(reader);
    }

    public void addLocal(String line) {
        empty = false;
        local.add(line);
    }

    public void addLocals(Collection<String> lines) {
        empty = false;
        local.addAll(lines);
    }

    public static PiqnicExtJenaIterator emptyIterator() {
        return new PiqnicExtJenaIterator();
    }

    private String bufferNext() {
        try {
            nextLine = local.poll();

            if(nextLine == null) {
                if(current == null) current = readers.poll();
                if(current == null) return null;

                nextLine = current.readLine();
                while (nextLine != null && seen.contains(nextLine)) {
                    nextLine = current.readLine();
                }
                if (nextLine != null && nextLine.startsWith(":")) {
                    PiqnicJenaConstants.NM += Integer.parseInt(nextLine.substring(1));
                    return bufferNext();
                }
                if(nextLine == null && readers.size() > 0) {
                    current.close();
                    current = readers.poll();
                    return bufferNext();
                }
            }
            seen.add(nextLine);
            return nextLine;
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public boolean hasNext() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if(empty) return false;
        boolean hasNext = nextLine != null || bufferNext() != null;

        if (!hasNext) {
            try {
                for(BufferedReader reader : readers) {
                    reader.close();
                }
            } catch (IOException e) {
                throw new IllegalStateException("I/O error when closing stream.", e);
            } catch (NullPointerException e) {
                return false;
            }
        }

        return hasNext;
    }

    @Override
    public Pair<Triple, Binding> next() {
        if(Thread.interrupted())
            throw new QueryInterruptedException("Interrupted.");
        if (!hasNext())
            throw new NoSuchElementException();

        if(PiqnicClient.test) {
            PiqnicJenaConstants.NTB += nextLine.getBytes().length;
        }
        String[] str = nextLine.split(";;");
        //System.out.println("Result: " + nextLine);

        Triple t;
        try {
            t = new Triple(
                    getNode(str[0]),
                    getNode(str[1]),
                    getNode(str[2]));
        } catch (ArrayIndexOutOfBoundsException e) {
            //System.out.println("ArrayIndexOutOfBound: " + nextLine);
            throw new NoSuchElementException();
        }
        BindingMap b = BindingFactory.create();
        for(int i = 3; i < str.length; i++) {
            b.add(Var.alloc(str[i].substring(1, str[i].indexOf("="))), getNode(str[i].substring(str[i].indexOf("=")+1)));
        }
        nextLine = null;

        return new Pair<>(t, b);
    }

    private Node getNode(String element) {
        if(element.length() == 0) return NodeFactory.createBlankNode();
        char firstChar = element.charAt(0);
        if (firstChar == '_') {
            return NodeFactory.createBlankNode(element);
        } else if (firstChar == '"') {
            String noq = element.replace("\"", "");
            if( noq.matches("-?\\d+")) {
                return Util.makeIntNode(Integer.parseInt(noq));
            } else if (noq.matches("([0-9]+)\\.([0-9]+)")) {
                return Util.makeDoubleNode(Double.parseDouble(noq));
            }
            return NodeFactory.createLiteral(element.replace("\"", ""));
        } else {
            return NodeFactory.createURI(element);
        }
    }
}
