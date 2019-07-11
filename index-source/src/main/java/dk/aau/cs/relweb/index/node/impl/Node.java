package com.anonymous.index.node.impl;

import com.anonymous.index.node.NodeBase;

import java.util.*;

public class Node extends NodeBase {
    public Node(UUID id, String ip, int port) {
        super(id, ip, port);
    }

    @Override
    public String toString() {
        return "Node{" +
                "id=" + id +
                ", ip='" + ip + '\'' +
                ", port=" + port +
                ", graphs=" + graphs +
                '}';
    }
}
