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

                //Jos pääavaimia on vain yksi, taulu on tavallinen taulu ja sille haetaan kaaret

                if(primaryKeysOfTable.size() == 1) {

                    //Haetaan taulun vierasavaimet ja käydään ne läpi

                    ResultSet foreignKeys = dbMetaData.getImportedKeys(schema, schema, tableName);

                    while (foreignKeys.next()) {

                        String fkTableName = foreignKeys.getString("FKTABLE_NAME");
                        String fkColumnName = foreignKeys.getString("FKCOLUMN_NAME");
                        String pkTableName = foreignKeys.getString("PKTABLE_NAME");
                        String pkColumnName = foreignKeys.getString("PKCOLUMN_NAME");


                        //Haetaan taulun pääavain-vierasavain -parit tietokannasta ja käydään ne läpi

                        ResultSet primayKeyForeignKeyValues = executeSQLQuery("SELECT " + primaryKeysOfTable.get(0) + "," + pkColumnName + " FROM " + schema + "." + tableName);

                        while (primayKeyForeignKeyValues.next()) {

                            //Haetaan vierasavaimen viitaaman taulun pääavaimet

                            ResultSet primaryKeysOfForeignTable = dbMetaData.getPrimaryKeys(schema, schema, fkTableName);
                            LinkedList<String> primaryKeysOfForeignTableList = new LinkedList<String>();

                            while (primaryKeysOfForeignTable.next()) {

                                primaryKeysOfForeignTableList.add(primaryKeysOfForeignTable.getString("COLUMN_NAME"));

                            }

                            //Jos viitatun taulun pääavaimen koko on 1, lisätään kaari

                            if (primaryKeysOfForeignTableList.size() == 1) {

                                //Kaareen lisätään käsitelävän taulun nimi, sen pääavain, vierastaulun nimi ja sen pääavain.

                                String currentTablePrimaryKeyValue = primayKeyForeignKeyValues.getString(primaryKeysOfTable.get(0));
                                String currentTableForeignColumnValue = primayKeyForeignKeyValues.getString(fkColumnName);

                                LinkedList<Edge> edges = new LinkedList<Edge>();

                                Edge edge = new Edge(false, tableName, currentTablePrimaryKeyValue, fkTableName, currentTableForeignColumnValue, schema);
                                edges.add(edge);

                                edgesOfTable.put(tableName, edges);

                            }

                        }

                    }

                } else if (primaryKeysOfTable.size() == 2) {

                    //Haetaan taulun vierasavaimet ja käydään ne läpi

                    ResultSet foreignKeys = dbMetaData.getImportedKeys(schema, schema, tableName);
                    HashMap<String, String>> referencedTables = new HashMap<String, String>();

                    while (foreignKeys.next()) {

                        String fkTableName = foreignKeys.getString("FKTABLE_NAME");
                        String fkColumnName = foreignKeys.getString("FKCOLUMN_NAME");
                        String pkTableName = foreignKeys.getString("PKTABLE_NAME");
                        String pkColumnName = foreignKeys.getString("PKCOLUMN_NAME");

                        referencedTables.add(fkColumnName, pkTableName);

                    }


                    if(foreignKeyColumns.size() == 2) {

                        ResultSet relationTableValues = executeSQLQuery("SELECT * FROM " + schema + "." + tableName);

                        while (relationTableValues.next()) {

                            String primaryKey1Value = relationTableValues.getString(primaryKeysOfTable.get(0));
                            String primaryKey2Value = relationTableValues.getString(primaryKeysOfTable.get(1));

                            int total_rows = relationTableValues.getMetaData().getColumnCount();
                            for (int i = 0; i < total_rows; i++) {
                                JSONObject obj = new JSONObject();
                                obj.put(resultSet.getMetaData().getColumnLabel(i + 1)
                                        .toLowerCase(), resultSet.getObject(i + 1));
                                jsonArray.put(obj);
                            }

                            LinkedList<Edge> edges = new LinkedList<Edge>();
                            Edge edge = new Edge(true, referencedTables.get(primaryKeysOfTable.get(0)), primaryKey1Value, referencedTables.get(primaryKeysOfTable.get(1)), primaryKey2Value, schema);
                            edges.add(edge);

                            edgesOfTable.put(tableName, edges);

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
