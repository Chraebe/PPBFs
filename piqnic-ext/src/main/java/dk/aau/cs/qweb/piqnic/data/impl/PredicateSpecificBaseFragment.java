package dk.aau.cs.qweb.piqnic.data.impl;

import com.google.gson.Gson;
import dk.aau.cs.qweb.piqnic.data.BaseFragmentBase;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.peer.Peer;
import dk.aau.cs.qweb.piqnic.util.Triple;

import java.io.File;
import java.util.Set;

public class PredicateSpecificBaseFragment extends BaseFragmentBase {
    public PredicateSpecificBaseFragment(String baseUri, String id, File file, Peer owner, Set<IPeer> clients) {
        super(baseUri, id, file, owner, clients);
    }

    @Override
    public boolean identify(Triple triplePattern) {
        return triplePattern.getPredicate().equals("ANY")
                || triplePattern.getPredicate().startsWith("?")
                || triplePattern.getPredicate().equals(getId());
    }

    @Override
    public String toString() {
        return toJSONString();
    }

    private String toJSONString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
