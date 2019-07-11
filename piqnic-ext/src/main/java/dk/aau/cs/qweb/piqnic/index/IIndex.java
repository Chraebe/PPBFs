package dk.aau.cs.qweb.piqnic.index;

import dk.aau.cs.qweb.piqnic.util.Triple;

import java.util.List;

public interface IIndex {
    void build();
    boolean isBuilt();
    IndexMapping getMapping(List<Triple> query);
}
