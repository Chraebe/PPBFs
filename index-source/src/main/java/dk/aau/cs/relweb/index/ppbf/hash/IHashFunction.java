package com.anonymous.index.ppbf.hash;

public interface IHashFunction {
    boolean isSingleValued();

    long hash(byte[] bytes);

    long[] hashMultiple(byte[] bytes);

}
