package graph;

import org.json.JSONArray;

import java.util.LinkedList;
import java.util.List;

public class Node {

    public Node(String graphName, String tablename, LinkedList<String> primaryKeyValues, JSONArray jsonArray) {

        this.graphName = graphName;
        this.tablename = tablename;
        this.primaryKeyValues = primaryKeyValues;
        this.jsonArray = jsonArray;
        this.connections = new LinkedList<Edge>();

    }

    private String graphName;

    public LinkedList<String> getPrimaryKeyValues() {
        return primaryKeyValues;
    }

    public void setPrimaryKeyValues(LinkedList<String> primaryKeyValues) {
        this.primaryKeyValues = primaryKeyValues;
    }

    private LinkedList<String> primaryKeyValues;

    public String getTableName() {
        return tablename;
    }

    public void setTableName(String tablename) {
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

        System.out.println("Node table " + tablename + " with values " + primaryKeyValues.toString() + " connections:");

        for (Edge edge : connections) {

            edge.print();

        }

        System.out.println();

    }

}
