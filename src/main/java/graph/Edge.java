package graph;

public class Edge {

    public Edge(boolean directed, String value, String table1name, String column1name, String table2name, String column2name, String graphName) {

        this.directed = directed;
        this.value = value;
        this.table1name = table1name;
        this.column1name = column1name;
        this.table2name = table2name;
        this.column2name = column2name;
        this.graphName = graphName;

    }


    private boolean directed;
    private String value;
    private String graphName;
    private String table1name;
    private String column1name;
    private String table2name;
    private String column2name;


    public void print() {

        //System.out.println("  Edge " + edgeId + " from node " + table1Id + " to node " + table2Id + ".");

    }

}
