package dk.aau.cs.qweb.piqnic.data;

import dk.aau.cs.qweb.piqnic.data.impl.PredicateSpecificFragment;
import dk.aau.cs.qweb.piqnic.data.impl.PredicateSpecificBaseFragment;
import dk.aau.cs.qweb.piqnic.data.impl.PredicateSpecificMetaFragment;
import dk.aau.cs.qweb.piqnic.peer.IPeer;
import dk.aau.cs.qweb.piqnic.peer.IndexPeer;
import dk.aau.cs.qweb.piqnic.peer.Peer;

import java.io.File;
import java.util.Set;

public class FragmentFactory {
    public static FragmentBase createFragment(String baseUri, String id, File file, Peer owner) {
        return new PredicateSpecificFragment(baseUri, id, file, owner);
    }

    public static BaseFragmentBase createBaseFragment(String baseUri, String id, File file, Peer owner, Set<IPeer> clients) {
        return new PredicateSpecificBaseFragment(baseUri, id, file, owner, clients);
    }

    public static MetaFragmentBase createMetaFragment(IndexPeer peer, String baseUri, String id) {
        return new PredicateSpecificMetaFragment(peer, baseUri, id);
    }
}
