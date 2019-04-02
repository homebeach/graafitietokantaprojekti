package graph;

public class Edge {

    public Edge(int edgeId, String node1Id, String node2Id, int graphId) {

        this.directed = directed;
        this.edgeId = edgeId;
        this.table1Id = table1Id;
        this.table2Id = table2Id;
    }

    public Edge(boolean directed, int edgeId, String table1Id, String table2Id, int graphId) {

        this.directed = directed;
        this.edgeId = edgeId;
        this.table1Id = table1Id;
        this.table2Id = table2Id;
    }


    private boolean directed;
    private int edgeId;
    private String table1Id;
    private String table2Id;

    public void print() {

        System.out.println("  Edge " + edgeId + " from node " + table1Id + " to node " + table2Id + ".");

    }

}
