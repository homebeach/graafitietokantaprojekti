package graph;

import org.json.JSONArray;

import java.util.List;

public class Node {

    public Node(String graphName, String nodeId, String tablename, JSONArray jsonArray, List<Edge> connections) {
        this.graphName = graphName;
        this.nodeId = nodeId;
        this.jsonArray = jsonArray;
        this.connections = connections;
    }


    private String graphName;

    private String nodeId;

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    private String tablename;

    private JSONArray jsonArray;

    private List<Edge> connections;

    public void print() {

        System.out.println("Node " + nodeId + " connections:");

        for (Edge edge : connections) {

            edge.print();

        }

    }

}
