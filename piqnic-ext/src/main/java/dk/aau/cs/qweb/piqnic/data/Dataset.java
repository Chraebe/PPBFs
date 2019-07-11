package dk.aau.cs.qweb.piqnic.data;

import dk.aau.cs.qweb.piqnic.peer.IPeer;

import java.util.ArrayList;
import java.util.List;

public class Dataset {
    private final String uri;
    private final List<BaseFragmentBase> fragments;
    private final IPeer owner;

    public Dataset(String uri, IPeer owner) {
        this.uri = uri;
        this.owner = owner;
        fragments = new ArrayList<>();
    }

    public Dataset(String uri, IPeer owner, List<BaseFragmentBase> fragments) {
        this.uri = uri;
        this.owner = owner;
        this.fragments = fragments;
    }

    public void addFragment(BaseFragmentBase fragment) {
        fragments.add(fragment);
    }

    public String getUri() {
        return uri;
    }

    public List<BaseFragmentBase> getFragments() {
        return fragments;
    }

    public IPeer getOwner() {
        return owner;
    }
}
