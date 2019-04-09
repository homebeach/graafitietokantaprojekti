package graph;

import java.sql.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import java.sql.ResultSet;
import java.util.Set;

public class LoadGraph {

    public LoadGraph(int graphId) {
        this.graphId = graphId;
    }

    private int graphId;

    private LinkedList<Node> nodes;

    private HashMap<String, LinkedList<String>> primaryKeysOfTables;

    private HashMap<String, LinkedList<Edge>> edgesOfTable;

    static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    static final String DB_URL = "jdbc:mariadb://127.0.0.1/graph";

    //  Database credentials
    static final String USERNAME = "root";
    static final String PASSWORD = "root";

    public LoadGraph() {
        this.nodes = new LinkedList<Node>();
        this.primaryKeysOfTables = new HashMap<String, LinkedList<String>>();
        this.edgesOfTable = new HashMap<String, LinkedList<Edge>>();
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

            System.out.println(1);
            while (resultSetTablesList.next()) {

                String tableName = resultSetTablesList.getString(3);

                LinkedList<Edge> edges = new LinkedList<Edge>();

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

                        String fkTableName = foreignKeyInformation.get("fkTableName");
                        String fkColumnName = foreignKeyInformation.get("fkColumnName");
                        String pkTableName = foreignKeyInformation.get("pkTableName");
                        String pkColumnName = foreignKeyInformation.get("pkColumnName");

                        //Haetaan taulun pääavain-vierasavain -parit tietokannasta ja käydään ne läpi

                        ResultSet primayKeyForeignKeyValues = executeSQLQuery("SELECT " + primaryKeysOfTable.get(0) + "," + fkColumnName + " FROM " + schema + "." + tableName + " LIMIT 10");

                        while (primayKeyForeignKeyValues.next()) {

                            //Haetaan vierasavaimen viitaaman taulun pääavaimet

                            ResultSet foreignKeysOfForeignTable = dbMetaData.getImportedKeys(schema, schema, pkTableName);
                            LinkedList<String> foreignKeysOfForeignTableList = new LinkedList<String>();

                            while (foreignKeysOfForeignTable.next()) {

                                foreignKeysOfForeignTableList.add(foreignKeysOfForeignTable.getString("COLUMN_NAME"));

                            }

                            System.out.println("tableName " + tableName);
                            System.out.println("fkTableName " + fkTableName);
                            System.out.println("fkColumnName " + fkColumnName);
                            System.out.println("pkTableName " + pkTableName);
                            System.out.println("pkColumnName " + pkColumnName);

                            System.out.println("foreignKeysOfForeignTableList.size() " + foreignKeysOfForeignTableList.size());
                            //Jos viitatun taulun pääavaimen koko on 1, lisätään kaari

                            if (foreignKeysOfForeignTableList.size() < 2) {

                                System.out.println("PRIMARY KEYS");

                                //Kaareen lisätään käsitelävän taulun nimi, sen pääavain, vierastaulun nimi ja sen pääavain.

                                String currentTablePrimaryKeyValue = primayKeyForeignKeyValues.getString(primaryKeysOfTable.get(0));
                                String currentTableForeignColumnValue = primayKeyForeignKeyValues.getString(fkColumnName);

                                Edge edge = new Edge(false, tableName, currentTablePrimaryKeyValue, fkTableName, currentTableForeignColumnValue, schema);
                                edges.add(edge);

                            }

                        }

                    }

                } else if (foreignKeysOfTable.size() > 1) {

                    //Haetaan taulun vierasavaimet ja käydään ne läpi

                    HashMap<String, String> referencedTables = new HashMap<String, String>();

                    for (int foreignKeyIndex : foreignKeysOfTable.keySet()) {

                        HashMap<String, String> foreignKeyInformation = foreignKeysOfTable.get(foreignKeyIndex);

                        String fkTableName = foreignKeyInformation.get("fkTableName");
                        String fkColumnName = foreignKeyInformation.get("fkColumnName");
                        String pkTableName = foreignKeyInformation.get("pkTableName");
                        String pkColumnName = foreignKeyInformation.get("pkColumnName");

                        referencedTables.put(fkColumnName, pkTableName);

                    }


                    if(referencedTables.size() == 2) {

                        Object[] keySet = referencedTables.keySet().toArray();
                        ResultSet relationTableValues = executeSQLQuery("SELECT * FROM " + schema + "." + tableName + " LIMIT 10");

                        while (relationTableValues.next()) {

                            String primaryKey1Value = relationTableValues.getString(keySet[0].toString());
                            String primaryKey2Value = relationTableValues.getString(keySet[1].toString());

                            int total_rows = relationTableValues.getMetaData().getColumnCount();

                            JSONArray jsonArray = new JSONArray();

                            for (int i = 0; i < total_rows; i++) {
                                JSONObject obj = new JSONObject();
                                obj.put(relationTableValues.getMetaData().getColumnLabel(i + 1)
                                        .toLowerCase(), relationTableValues.getObject(i + 1));
                                jsonArray.put(obj);
                            }

                            Edge edge = new Edge(true, tableName, referencedTables.get(primaryKeysOfTable.get(0)), primaryKey1Value, referencedTables.get(primaryKeysOfTable.get(1)), primaryKey2Value, jsonArray, schema);
                            edges.add(edge);

                        }

                    }

                }

                edgesOfTable.put(tableName, edges);
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

    public void getNodes(String schema) throws Exception {

        System.out.println("Graphid: " + graphId + ".");

        for (String table: primaryKeysOfTables.keySet()) {

            LinkedList<String> keys = primaryKeysOfTables.get(table);

            System.out.println("Keys for the table: " + table);

            if(keys.size() == 1) {

                ResultSet resultSet = executeSQLQuery("SELECT * FROM " + schema + "." + table + " LIMIT 10");


                while (resultSet.next()) {

                    int total_rows = resultSet.getMetaData().getColumnCount();

                    JSONArray jsonArray = new JSONArray();

                    for (int i = 0; i < total_rows; i++) {
                        JSONObject obj = new JSONObject();
                        obj.put(resultSet.getMetaData().getColumnLabel(i + 1)
                                .toLowerCase(), resultSet.getObject(i + 1));
                        jsonArray.put(obj);
                    }

                    LinkedList<Edge> connections = edgesOfTable.get(table);

                    String nodeId = resultSet.getString(keys.get(0));

                    Node node = new Node(schema, nodeId, table, jsonArray, connections);

                    //nodes.add(node);
                    
                }

            }

        }

    }

    public void printEdges() {

        for (String table: edgesOfTable.keySet()) {

            LinkedList<Edge> edges = edgesOfTable.get(table);

            for (Edge edge : edges) {
                edge.print();
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
