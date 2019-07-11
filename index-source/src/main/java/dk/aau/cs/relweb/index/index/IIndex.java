package com.anonymous.index.index;

import com.anonymous.index.util.Triple;

import java.util.List;

public interface IIndex {
    boolean isBuilt();
    IndexMapping getMapping(List<Triple> query);
}
