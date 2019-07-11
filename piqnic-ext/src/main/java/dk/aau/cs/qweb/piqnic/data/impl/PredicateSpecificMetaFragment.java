package dk.aau.cs.qweb.piqnic.data.impl;

import com.google.gson.Gson;
import dk.aau.cs.qweb.piqnic.data.MetaFragmentBase;
import dk.aau.cs.qweb.piqnic.peer.IndexPeer;
import dk.aau.cs.qweb.piqnic.util.Triple;

public class PredicateSpecificMetaFragment extends MetaFragmentBase {
    public PredicateSpecificMetaFragment(IndexPeer node, String baseUri, String id) {
        super(node, baseUri, id);
    }

    @Override
    public boolean identify(Triple triplePattern) {
        return triplePattern.getPredicate().equals("ANY")
                || triplePattern.getPredicate().startsWith("?")
                || triplePattern.getPredicate().equals(getId());
    }

    @Override
    public int hashCode() {
        return getBaseUri().hashCode() + getId().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj.getClass() != getClass()) return false;
        PredicateSpecificMetaFragment other = (PredicateSpecificMetaFragment) obj;
        return getBaseUri().equals(other.getBaseUri()) && getId().equals(other.getId());
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
