package graph;

import org.json.JSONArray;

import java.util.List;

public class Node {

    public Node(int graphId, int nodeId, JSONArray jsonArray, List<Edge> connections) {
        this.graphId = graphId;
        this.nodeId = nodeId;
        this.jsonArray = jsonArray;
        this.connections = connections;
    }

    public Node(int graphId, int nodeId, JSONArray jsonArray) {
        this.graphId = graphId;
        this.nodeId = nodeId;
        this.jsonArray = jsonArray;
    }


    int graphId;

    int nodeId;

    JSONArray jsonArray;

    private List<Edge> connections;

    public void print() {

        System.out.println("Node " + nodeId + " connections:");

        for (Edge edge : connections) {

            edge.print();

        }

    }

}
