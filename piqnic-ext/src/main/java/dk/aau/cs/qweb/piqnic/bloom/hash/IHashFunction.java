package dk.aau.cs.qweb.piqnic.bloom.hash;

public interface IHashFunction {
    boolean isSingleValued();

    long hash(byte[] bytes);

    long[] hashMultiple(byte[] bytes);

}
