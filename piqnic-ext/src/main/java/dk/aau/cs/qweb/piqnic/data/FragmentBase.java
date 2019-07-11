package dk.aau.cs.qweb.piqnic.data;

import com.google.gson.Gson;
import dk.aau.cs.qweb.piqnic.PiqnicClient;
import dk.aau.cs.qweb.piqnic.bloom.IBloomFilter;
import dk.aau.cs.qweb.piqnic.bloom.PrefixPartitionedBloomFilter;
import dk.aau.cs.qweb.piqnic.node.PiqnicNode;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.peer.IndexPeer;
import dk.aau.cs.qweb.piqnic.peer.Peer;

import java.io.File;
import java.util.Objects;
import java.util.Set;

public abstract class FragmentBase implements IFragment {
    private final String baseUri;
    private final String id;
    private final File file;
    private final Peer owner;
    private IBloomFilter<String> bloom = PrefixPartitionedBloomFilter.empty();

    public FragmentBase(String baseUri, String id, File file, Peer owner) {
        this.baseUri = baseUri;
        this.id = id;
        this.file = file;
        this.owner = owner;
    }

    public Peer getOwner() {
        return owner;
    }

    public String getBaseUri() {
        return baseUri;
    }

    public String getId() {
        return id;
    }

    public File getFile() {
        return file;
    }

    public String getIdString() {
        return baseUri + ":" + id;
    }

    public IBloomFilter<String> getBloom() {
        if(bloom.isEmpty()) bloom = PrefixPartitionedBloomFilter.create(0.1, file.getAbsolutePath()+".ppbf");
        return bloom;
    }

    public void buildBloom() {
        File f = new File(file+".ppbf");
        if(f.exists()) bloom =  PrefixPartitionedBloomFilter.create(f.getAbsolutePath());
        else bloom = PiqnicClient.nodeInstance.getConstituentsBloomFilter(this);
    }

    public void rebuildBloom() {
        bloom.deleteFile();
        bloom = PiqnicClient.nodeInstance.getConstituentsBloomFilter(this);
    }

    public boolean ownedBy(Peer peer) {
        return owner.equals(peer);
    }

    public BaseFragmentBase toBaseFragment(Set<IPeer> peers) {
        return FragmentFactory.createBaseFragment(baseUri, id, file, owner, peers);
    }

    public MetaFragmentBase toMetaFragment() {
        //return FragmentFactory.createMetaFragment(new IndexPeer(owner), baseUri, id);
        return FragmentFactory.createMetaFragment(new IndexPeer((PiqnicNode)PiqnicClient.nodeInstance), baseUri, id);
    }

    @Override
    public String toString() {
        return toJSONString();
    }

    private String toJSONString() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FragmentBase that = (FragmentBase) o;
        return Objects.equals(baseUri, that.baseUri) &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseUri, id);
    }
}
