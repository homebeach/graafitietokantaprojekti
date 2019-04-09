package graph;

import org.json.JSONArray;

public class Edge {

    public Edge(boolean directed, String primaryKeyTableName, String primaryKeyColumnValue, String foreignKeyTableName, String foreignKeyTableValue, String graphName) {

        this.directed = directed;
        this.primaryKeyTableName = primaryKeyTableName;
        this.primaryKeyColumnValue = primaryKeyColumnValue;
        this.foreignKeyTableName = foreignKeyTableName;
        this.foreignKeyTableValue = foreignKeyTableValue;
        this.graphName = graphName;

    }

    public Edge(boolean directed, String role, String primaryKeyTableName, String primaryKeyColumnValue, String foreignKeyTableName, String foreignKeyTableValue, JSONArray jsonArray, String graphName) {

        this.directed = directed;
        this.role = role;
        this.primaryKeyTableName = primaryKeyTableName;
        this.primaryKeyColumnValue = primaryKeyColumnValue;
        this.foreignKeyTableName = foreignKeyTableName;
        this.foreignKeyTableValue = foreignKeyTableValue;
        this.jsonArray = jsonArray;
        this.graphName = graphName;

    }

    private boolean directed;
    private String role;
    private String primaryKeyTableName;
    private String primaryKeyColumnValue;
    private String foreignKeyTableName;
    private String foreignKeyTableValue;
    private JSONArray jsonArray;
    private String graphName;

    public void print() {

        System.out.println("Role: " + role + " Pktable " + primaryKeyTableName + " pkvalue " + primaryKeyColumnValue + " fktable " + foreignKeyTableName + " fkvalue " + foreignKeyTableValue);

    }

}
