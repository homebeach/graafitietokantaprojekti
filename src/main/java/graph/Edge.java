package graph;

import org.json.JSONArray;

import java.util.LinkedList;

public class Edge {

    public Edge(boolean directed, String relation, String table1Name, LinkedList<String> table1PrimaryKeyValues, String table2Name, LinkedList<String> table2PrimaryKeyValues, String graphName) {

        this.directed = directed;
        this.relation = relation;
        this.table1Name = table1Name;
        this.table1PrimaryKeyValues = table1PrimaryKeyValues;
        this.table2Name = table2Name;
        this.table2PrimaryKeyValues = table2PrimaryKeyValues;
        this.graphName = graphName;

    }

    public Edge(boolean directed, String relation, String table1Name, LinkedList<String> table1PrimaryKeyValues, String table2Name, LinkedList<String> table2PrimaryKeyValues, JSONArray jsonArray, String graphName) {

        this.directed = directed;
        this.relation = relation;
        this.table1Name = table1Name;
        this.table1PrimaryKeyValues = table1PrimaryKeyValues;
        this.table2Name = table2Name;
        this.table2PrimaryKeyValues = table2PrimaryKeyValues;
        this.jsonArray = jsonArray;
        this.graphName = graphName;

    }

    private boolean directed;
    private String relation;

    public String getTable1Name() {
        return table1Name;
    }

    public void setTable1Name(String table1Name) {
        this.table1Name = table1Name;
    }

    public String getTable2Name() {
        return table2Name;
    }

    public void setTable2Name(String table2Name) {
        this.table2Name = table2Name;
    }

    private String table1Name;

    public LinkedList<String> getTable1PrimaryKeyValues() {
        return table1PrimaryKeyValues;
    }

    public void setTable1PrimaryKeyValues(LinkedList<String> table1PrimaryKeyValues) {
        this.table1PrimaryKeyValues = table1PrimaryKeyValues;
    }

    private LinkedList<String> table1PrimaryKeyValues;

    private String table2Name;

    public LinkedList<String> getTable2PrimaryKeyValues() {
        return table2PrimaryKeyValues;
    }

    public void setTable2PrimaryKeyValues(LinkedList<String> table2PrimaryKeyValues) {
        this.table2PrimaryKeyValues = table2PrimaryKeyValues;
    }

    private LinkedList<String> table2PrimaryKeyValues;
    private JSONArray jsonArray;
    private String graphName;

    public void print() {

        if(jsonArray != null) {
            System.out.println("Relation: " + relation + "  Table1_name: " + table1Name + " primary key values: " + table1PrimaryKeyValues.toString() + "  Table2_name: " + table2Name + " primary key values: " + table2PrimaryKeyValues.toString() + " and content: " + jsonArray.toString());
        } else {
            System.out.println("Relation: " + relation + "  Table1_name: " + table1Name + " primary key values: " + table1PrimaryKeyValues.toString() + "  Table2_name: " + table2Name + " primary key values: " + table2PrimaryKeyValues.toString());
        }

    }

}
