package main.java.graph;

public class Edge {

    public Edge(int edgeId, int node1Id, int node2Id, int graphId) {
        this.node1Id = node1Id;
        this.node2Id = node2Id;
    }

    private int edgeId;
    private int node1Id;
    private int node2Id;
    private int graphId;

}
