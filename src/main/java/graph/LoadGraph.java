package graph;

import java.sql.*;
import java.util.*;

import org.json.JSONArray;
import org.json.JSONObject;
import java.sql.ResultSet;

public class LoadGraph {

    public LoadGraph(int graphId) {
        this.graphId = graphId;
    }

    private int graphId;

    private LinkedList<Node> nodes;

    private LinkedList<Edge> edges;

    private HashMap<String, LinkedList<String>> primaryKeysOfTables;

    private HashMap<String, LinkedList<Edge>> edgesOfTable;

    static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    static final String DB_URL = "jdbc:mariadb://127.0.0.1/graph";

    //  Database credentials
    static final String USERNAME = "root";
    static final String PASSWORD = "root";

    public LoadGraph() {
        this.nodes = new LinkedList<Node>();
        this.edges = new LinkedList<Edge>();
        this.primaryKeysOfTables = new HashMap<String, LinkedList<String>>();
    }


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

    public void getEdges(String schema) {

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSetTablesList = null;

        try {

            Class.forName(JDBC_DRIVER );
            conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
            DatabaseMetaData dbMetaData = conn.getMetaData();
            String[] types = {"TABLE"};
            resultSetTablesList = dbMetaData.getTables(schema, null, "%", types);

            //Tutkitaan skeemassa olevien taulujen joukkoa

            while (resultSetTablesList.next()) {

                String tableName = resultSetTablesList.getString(3);

                //Haetaan käsiteltävän taulun pääavaimet

                ResultSet resultSetPrimaryKeysList = dbMetaData.getPrimaryKeys(schema, schema, tableName);
                LinkedList<String> primaryKeysOfTable = new LinkedList<String>();

                while (resultSetPrimaryKeysList.next()) {

                    primaryKeysOfTable.add(resultSetPrimaryKeysList.getString("COLUMN_NAME"));

                }

                ResultSet foreignKeys = dbMetaData.getImportedKeys(schema, schema, tableName);

                HashMap<Integer, HashMap<String, String>> foreignKeysOfTable = new HashMap<Integer, HashMap<String, String>>();

                int foreignKeysCounter = 0;
                while (foreignKeys.next()) {

                    String fkTableName = foreignKeys.getString("FKTABLE_NAME");
                    String fkColumnName = foreignKeys.getString("FKCOLUMN_NAME");
                    String pkTableName = foreignKeys.getString("PKTABLE_NAME");
                    String pkColumnName = foreignKeys.getString("PKCOLUMN_NAME");

                    HashMap<String, String> foreignKeysInformation = new HashMap<String, String>();

                    foreignKeysInformation.put("fkTableName", fkTableName);
                    foreignKeysInformation.put("fkColumnName", fkColumnName);
                    foreignKeysInformation.put("pkTableName", pkTableName);
                    foreignKeysInformation.put("pkColumnName", pkColumnName);

                    foreignKeysOfTable.put(foreignKeysCounter, foreignKeysInformation);
                    foreignKeysCounter++;
                }


                //Jos pääavaimia on vain yksi, taulu on tavallinen taulu ja sille haetaan kaaret
                if(foreignKeysOfTable.size() == 1) {

                    //Haetaan taulun vierasavaimet ja käydään ne läpi

                    for (int foreignKeyIndex : foreignKeysOfTable.keySet()) {

                        HashMap<String, String> foreignKeyInformation = foreignKeysOfTable.get(foreignKeyIndex);

                        String fkColumnName = foreignKeyInformation.get("fkColumnName");
                        String foreignTableName = foreignKeyInformation.get("pkTableName");
                        String pkColumnName = foreignKeyInformation.get("pkColumnName");

                        //Haetaan taulun pääavain-vierasavain -parit tietokannasta ja käydään ne läpi

                        String primaryKeys = String.join(",", primaryKeysOfTable);

                        ResultSet primaryKeyForeignKeyValues = executeSQLQuery("SELECT " + primaryKeys + "," + fkColumnName + " FROM " + schema + "." + tableName + " LIMIT 10");

                        while (primaryKeyForeignKeyValues.next()) {

                            //Haetaan vierasavaimen viitaaman taulun pääavaimet

                            ResultSet foreignKeysOfForeignTable = dbMetaData.getImportedKeys(schema, schema, foreignTableName);
                            LinkedList<String> foreignKeysOfForeignTableList = new LinkedList<String>();

                            while (foreignKeysOfForeignTable.next()) {

                                foreignKeysOfForeignTableList.add(foreignKeysOfForeignTable.getString("COLUMN_NAME"));

                            }

                            //Jos viitatun taulun pääavaimen koko on 1, lisätään kaari

                            if (foreignKeysOfForeignTableList.size() < 2) {

                                //Kaareen lisätään käsitelävän taulun nimi, sen pääavain, vierastaulun nimi ja sen pääavain.

                                LinkedList<String> primaryKeysOfTableValues = new LinkedList<String>();
                                for (int i=0; i<primaryKeysOfTable.size(); i++) {
                                    primaryKeysOfTableValues.add(primaryKeyForeignKeyValues.getString(primaryKeysOfTable.get(i)));
                                }

                                LinkedList<String> foreignKeysOfTableValues = new LinkedList<String>();
                                foreignKeysOfTableValues.add(primaryKeyForeignKeyValues.getString(fkColumnName));

                                Edge edge = new Edge(false,"1toN", tableName, primaryKeysOfTableValues, foreignTableName, foreignKeysOfTableValues, schema);
                                edges.add(edge);

                            }

                        }

                    }

                } else if (foreignKeysOfTable.size() > 1) {

                    //Haetaan taulun vierasavaimet ja käydään ne läpi

                    HashMap<String, String> referencedTables = new HashMap<String, String>();

                    for (int foreignKeyIndex : foreignKeysOfTable.keySet()) {

                        HashMap<String, String> foreignKeyInformation = foreignKeysOfTable.get(foreignKeyIndex);

                        String fkColumnName = foreignKeyInformation.get("fkColumnName");
                        String foreignTableName = foreignKeyInformation.get("pkTableName");

                        referencedTables.put(fkColumnName, foreignTableName);

                    }


                    if(referencedTables.size() == 2) {

                        Object[] keySet = referencedTables.keySet().toArray();
                        ResultSet relationTableValues = executeSQLQuery("SELECT * FROM " + schema + "." + tableName + " LIMIT 10");

                        while (relationTableValues.next()) {

                            String table1Value = relationTableValues.getString(keySet[0].toString());
                            String table2Value = relationTableValues.getString(keySet[1].toString());

                            LinkedList<String> table1Values = new LinkedList<String>();
                            table1Values.add(table1Value);

                            LinkedList<String> table2Values = new LinkedList<String>();
                            table2Values.add(table2Value);

                            int total_rows = relationTableValues.getMetaData().getColumnCount();

                            JSONArray jsonArray = new JSONArray();

                            for (int i = 0; i < total_rows; i++) {
                                JSONObject obj = new JSONObject();
                                obj.put(relationTableValues.getMetaData().getColumnLabel(i + 1)
                                        .toLowerCase(), relationTableValues.getObject(i + 1));
                                jsonArray.put(obj);
                            }

                            Edge edge = new Edge(true, tableName, referencedTables.get(primaryKeysOfTable.get(0)), table1Values, referencedTables.get(primaryKeysOfTable.get(1)), table2Values, jsonArray, schema);
                            edges.add(edge);

                        }

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

    public void getNodes(String schema) {

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSetTablesList = null;

        try {

            Class.forName(JDBC_DRIVER );
            conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
            DatabaseMetaData dbMetaData = conn.getMetaData();
            String[] types = {"TABLE"};
            resultSetTablesList = dbMetaData.getTables(schema, null, "%", types);

            while (resultSetTablesList.next()) {

                String tableName = resultSetTablesList.getString(3);

                ResultSet resultSetPrimaryKeysList = dbMetaData.getPrimaryKeys(schema, schema, tableName);
                LinkedList<String> primaryKeysOfTable = new LinkedList<String>();

                while (resultSetPrimaryKeysList.next()) {
                    primaryKeysOfTable.add(resultSetPrimaryKeysList.getString("COLUMN_NAME"));
                }

                String primaryKeys = String.join(",", primaryKeysOfTable);

                ResultSet resultSet = executeSQLQuery("SELECT " + primaryKeys + " FROM " + schema + "." + tableName + " LIMIT 10");

                while (resultSet.next()) {

                    int total_rows = resultSet.getMetaData().getColumnCount();

                    JSONArray jsonArray = new JSONArray();

                    for (int i = 0; i < total_rows; i++) {
                        JSONObject obj = new JSONObject();
                        obj.put(resultSet.getMetaData().getColumnLabel(i + 1)
                                .toLowerCase(), resultSet.getObject(i + 1));
                        jsonArray.put(obj);
                    }

                    LinkedList<String> primaryKeysOfTableValues = new LinkedList<String>();

                    for(int i=1; i<=primaryKeysOfTable.size(); i++) {
                        primaryKeysOfTableValues.add(resultSet.getString(i));
                    }

                    Node node = new Node(schema, tableName, primaryKeysOfTableValues, jsonArray);
                    nodes.add(node);

                }

            }

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

    }

    public void getEdgesForNodes(String schema) {

        if (nodes != null && edges != null) {

            for (Node node : nodes) {

                for (Edge edge : edges) {

                    LinkedList<String> primaryKeyValues = node.getPrimaryKeyValues();
                    Collections.sort(primaryKeyValues);

                    LinkedList<String> table1PrimaryKeyValues = edge.getTable1PrimaryKeyValues();
                    Collections.sort(table1PrimaryKeyValues);

                    LinkedList<String> table2PrimaryKeyValues = edge.getTable2PrimaryKeyValues();
                    Collections.sort(table2PrimaryKeyValues);

                    if (primaryKeyValues.equals(table1PrimaryKeyValues) || primaryKeyValues.equals(table2PrimaryKeyValues)) {
                        node.getConnections().add(edge);
                    }

                }

            }

        } else {
            System.out.println("You must load edges and nodes first!");
        }

    }

    public void printEdges() {

        for (Edge edge : edges) {
            edge.print();
        }

    }

    public void printNodes() {

        for (Node node : nodes) {
            node.print();
        }

    }

    public void printGraph() {

        System.out.println("Graphid: " + graphId + ".");

        for (Node node : nodes) {
            node.print();
        }

    }
}
