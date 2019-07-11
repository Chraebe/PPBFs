package dk.aau.cs.qweb.piqnic.data;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.peer.IndexPeer;

public abstract class MetaFragmentBase implements IFragment {
    @Expose
    private IndexPeer node;
    @Expose
    private String baseUri;
    @Expose
    private String id;

    public MetaFragmentBase(IndexPeer node, String baseUri, String id) {
        this.node = node;
        this.baseUri = baseUri;
        this.id = id;
    }

    public IPeer getNode() {
        return node;
    }

    public String getBaseUri() {
        return baseUri;
    }

    public String getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return baseUri.hashCode() + id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(obj.getClass() != MetaFragmentBase.class) return false;
        MetaFragmentBase other = (MetaFragmentBase) obj;
        return baseUri.equals(other.baseUri) && id.equals(other.id);
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
