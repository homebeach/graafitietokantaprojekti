package main.java.graph;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.sql.*;

public class Graph {

    private int graphId;

    private HashMap<Integer, Node> nodes = new HashMap<Integer, Node>();

    static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    static final String DB_URL = "jdbc:mariadb://127.0.0.1/db";

    //  Database credentials
    static final String USERNAME = "root";
    static final String PASSWORD = "root";

    public void loadNodes(ResultSet nodes) {

        try {

            while (nodes.next()) {

                int nodeId = nodes.getInt("node_id");
                ResultSet edges = executeSQLQuery("SELECT * FROM graph.nodes WHERE node_id=" + nodeId);
                List<Edge> connections = loadEdges(edges);
                Node node = new Node(nodeId, connections);

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

                Edge edge = new Edge(edgeId, node1Id, node2Id, graphId);

                connections.add(edge);

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

            System.out.println("Connecting to a selected database.");
            conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
            System.out.println("Connected database successfully.");

            System.out.println("Creating table in given database...");
            stmt = conn.createStatement();

            resultSet = stmt.executeQuery(sqlQuery);

            System.out.println("Created table in given database...");
        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
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
        System.out.println("Goodbye!");

    return resultSet;
    }



    public void loadGraph() {

        ResultSet nodes = executeSQLQuery("SELECT * FROM graph.node");

        loadNodes(nodes);

    }
}
