package graph;

import java.sql.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import java.sql.ResultSet;

public class LoadGraph {

    public LoadGraph(int graphId) {
        this.graphId = graphId;
    }

    private int graphId;

    private LinkedList<Node> nodes = new LinkedList<Node>();

    private HashMap<String, LinkedList<String>> primaryKeysOfTables = new HashMap<String, LinkedList<String>>();

    private HashMap<String, LinkedList<Edge>> edgesOfTable = new HashMap<String, LinkedList<Edge>>();

    static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    static final String DB_URL = "jdbc:mariadb://127.0.0.1/graph";

    //  Database credentials
    static final String USERNAME = "root";
    static final String PASSWORD = "root";


    public ResultSet executeSQLQuery(String sqlQuery) {

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        try {

            Class.forName(JDBC_DRIVER);

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

    public ResultSet getTablesAndKeysForSchema(String schema) {

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        try {

            Class.forName(JDBC_DRIVER );

            conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);

            DatabaseMetaData dbMetaData = conn.getMetaData();

            String[] types = {"TABLE"};

            resultSet = dbMetaData.getTables(schema, null, "%", types);

            while (resultSet.next()) {

                    String tableName = resultSet.getString(3);
                    ResultSet rs2 = dbMetaData.getPrimaryKeys(schema, schema, tableName);

                    LinkedList<String> primaryKeys = new LinkedList<String>();

                    ResultSet foreignKeys = dbMetaData.getImportedKeys(schema, schema, tableName);

                    while (rs2.next()) {

                        primaryKeys.add(rs2.getString("COLUMN_NAME"));
                        primaryKeysOfTables.put(tableName, primaryKeys);

                    }

                    while (foreignKeys.next()) {

                        String fkTableName = foreignKeys.getString("FKTABLE_NAME");
                        String fkColumnName = foreignKeys.getString("FKCOLUMN_NAME");
                        String pkTableName = foreignKeys.getString("PKTABLE_NAME");
                        String pkColumnName = foreignKeys.getString("PKCOLUMN_NAME");

                        ResultSet pkColumnValues = executeSQLQuery("SELECT " + pkColumnName + " FROM " + schema + "." + pkTableName);

                        while (pkColumnValues.next()) {

                            LinkedList<Edge> edges = new LinkedList<Edge>();

                            String fkColumnValue = pkColumnValues.getString(fkColumnName);

                            Edge edge = null;

                            if (primaryKeys.size() > 1) {

                                edge = new Edge(true, fkColumnValue, fkTableName, fkColumnName, pkTableName, pkColumnName, schema);

                            } else {

                                edge = new Edge(false, fkColumnValue, fkTableName, fkColumnName, pkTableName, pkColumnName, schema);

                            }

                            edges.add(edge);

                            edgesOfTable.put(tableName, edges);

                        }

                    }

            }

            conn.close();


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

        //ResultSet nodes = executeSQLQuery("SELECT * FROM graph.nodes WHERE nodes.graph_id=" + graphId);
        //loadNodes(nodes);

    }

    public void printKeys() {

        for (String table: primaryKeysOfTables.keySet()){

            LinkedList<String> keys = primaryKeysOfTables.get(table);

            System.out.println("Keys for the table: " + table);

            for (String key : keys) {
                System.out.println(key);
            }

        }

    }

    public void getNodes(String schema) throws Exception {

        System.out.println("Graphid: " + graphId + ".");

        for (String table: primaryKeysOfTables.keySet()) {

            LinkedList<String> keys = primaryKeysOfTables.get(table);

            System.out.println("Keys for the table: " + table);

            if(keys.size() == 1) {

                ResultSet resultSet = executeSQLQuery("SELECT * FROM " + schema + "." + table);

                JSONArray jsonArray = new JSONArray();
                while (resultSet.next()) {
                    int total_rows = resultSet.getMetaData().getColumnCount();
                    for (int i = 0; i < total_rows; i++) {
                        JSONObject obj = new JSONObject();
                        obj.put(resultSet.getMetaData().getColumnLabel(i + 1)
                                .toLowerCase(), resultSet.getObject(i + 1));
                        jsonArray.put(obj);
                    }

                    LinkedList<Edge> connections = edgesOfTable.get(table);

                    String nodeId = resultSet.getString(keys.get(0));

                    Node node = new Node(schema, nodeId, table, jsonArray, connections);

                    nodes.add(node);
                    
                }

            }

        }

    }

    public void printGraph() {

        System.out.println("Graphid: " + graphId + ".");

        for (Node node : nodes) {
            node.print();
        }

    }
}
