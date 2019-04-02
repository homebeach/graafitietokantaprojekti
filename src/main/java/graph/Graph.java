package graph;

import java.sql.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;


public class Graph {

    public Graph(int graphId) {
        this.graphId = graphId;
    }

    private int graphId;

    private LinkedList<Node> nodes;

    static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    static final String DB_URL = "jdbc:mariadb://127.0.0.1/";

    //  Database credentials
    static final String USERNAME = "root";
    static final String PASSWORD = "root";

    public void loadNodes(ResultSet rsNodes) {

        nodes = new LinkedList<Node>();

        try {

            while (rsNodes.next()) {

                int graphId = rsNodes.getInt("graph_id");
                int nodeId = rsNodes.getInt("node_id");
                ResultSet edges = executeSQLQuery("SELECT * FROM graph.edges WHERE edges.from_node_id=" + nodeId);
                List<Edge> connections = loadEdges(edges);
                Node node = new Node(graphId, nodeId, connections);
                nodes.add(node);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public List<Edge> loadEdges(ResultSet edges) {

        List<Edge> connections = null;

        try {

            connections = new LinkedList<Edge>();

            while (edges.next()) {

                int edgeId = edges.getInt("edge_id");
                int node1Id = edges.getInt("from_node_id");
                int node2Id = edges.getInt("to_node_id");
                int graphId = edges.getInt("graph_id");

               // Edge edge = new Edge(edgeId, node1Id, node2Id, graphId);

                //connections.add(edge);

            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

    return connections;
    }

    public ResultSet executeSQLQuery(String sqlQuery) {

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        try {

            Class.forName(JDBC_DRIVER );

            conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
            stmt = conn.createStatement();

            resultSet = stmt.executeQuery(sqlQuery);

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                if (stmt != null) {
                    conn.close();
                }
            } catch (SQLException se) {
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }

    return resultSet;
    }



    public void loadGraph() {

        ResultSet nodes = executeSQLQuery("SELECT * FROM graph.nodes WHERE nodes.graph_id=" + graphId);
        loadNodes(nodes);

    }

    public void printGraph() {

        System.out.println("Graphid: " + graphId + ".");

        for (Node node : nodes) {
            node.print();
        }

    }
}
