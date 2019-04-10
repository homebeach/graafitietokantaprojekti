package graph;

import org.json.JSONArray;

import java.util.LinkedList;
import java.util.List;

public class Node {

    public Node(String graphName, LinkedList<String> primaryKeys, String tablename, JSONArray jsonArray) {
        this.graphName = graphName;
        this.primaryKeys = primaryKeys;
        this.jsonArray = jsonArray;
        this.connections = connections;
    }

    private String graphName;

    public LinkedList<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(LinkedList<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    private LinkedList<String> primaryKeys;

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    private String tablename;

    private JSONArray jsonArray;

    public List<Edge> getConnections() {
        return connections;
    }

    public void setConnections(List<Edge> connections) {
        this.connections = connections;
    }

    private List<Edge> connections;

    public void print() {

        System.out.println("Node " + primaryKeys.toString() + " connections:");

        for (Edge edge : connections) {

            edge.print();

        }

    }

}
