package graph;

import java.util.List;

public class Node {

    public Node(int graphId, int nodeId, List<Edge> connections) {
        this.graphId = graphId;
        this.nodeId = nodeId;
        this.connections = connections;
    }

    int graphId;

    int nodeId;

    private List<Edge> connections;

    public void print() {

        System.out.println("Node " + nodeId + " connections:");

        for (Edge edge : connections) {

            edge.print();

        }

    }

}
