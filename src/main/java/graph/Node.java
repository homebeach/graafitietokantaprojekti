package main.java.graph;

import java.util.List;

public class Node {

    public Node(int nodeId, List<Edge> connections) {
        this.nodeId = nodeId;
        this.connections = connections;
    }

    int nodeId;

    private List<Edge> connections;

}
