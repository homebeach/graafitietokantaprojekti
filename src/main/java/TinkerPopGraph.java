import graph.Edge;
import graph.Node;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

public class TinkerPopGraph {

    public TinkerPopGraph(int graphId) {
        this.graphId = graphId;
    }

    private int graphId;

    private HashMap<String, LinkedList<String>> primaryKeysOfTables;

    private LinkedList<Vertex> vertexes;

    private TinkerGraph tinkerGraph;

    static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    static final String DB_URL = "jdbc:mariadb://127.0.0.1/";

    //  Database credentials
    static final String USERNAME = "root";
    static final String PASSWORD = "root";

    public TinkerPopGraph() {
        this.vertexes = new LinkedList<Vertex>();
        this.primaryKeysOfTables = new HashMap<String, LinkedList<String>>();
        this.tinkerGraph = TinkerGraph.open();
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

        System.out.println("Loading edges");

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

                boolean foreignKeysArePrimaryKeys = true;

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

                    if(!primaryKeysOfTable.contains(fkColumnName)) {
                        foreignKeysArePrimaryKeys = false;
                    }

                }

                LinkedList<String> foreignKeysOfTableValues = new LinkedList<String>();


                if(!foreignKeysArePrimaryKeys) {

                    //Haetaan taulun vierasavaimet ja käydään ne läpi

                    for (int foreignKeyIndex : foreignKeysOfTable.keySet()) {

                        HashMap<String, String> foreignKeyInformation = foreignKeysOfTable.get(foreignKeyIndex);

                        String fkColumnName = foreignKeyInformation.get("fkColumnName");
                        String foreignTableName = foreignKeyInformation.get("pkTableName");
                        String pkColumnName = foreignKeyInformation.get("pkColumnName");

                        //Haetaan taulun pääavain-vierasavain -parit tietokannasta ja käydään ne läpi

                        String primaryKeys = String.join(",", primaryKeysOfTable);

                        //ResultSet primaryKeyForeignKeyValues = executeSQLQuery("SELECT " + primaryKeys + "," + fkColumnName + " FROM " + schema + "." + tableName + " ORDER BY " + primaryKeys + " LIMIT 10");
                        ResultSet primaryKeyForeignKeyValues = executeSQLQuery("SELECT " + primaryKeys + "," + fkColumnName + " FROM " + schema + "." + tableName + " ORDER BY " + primaryKeys);

                        LinkedList<Edge> edges = new LinkedList<Edge>();

                        while (primaryKeyForeignKeyValues.next()) {

                            //Haetaan vierasavaimen viitaaman taulun pääavaimet

                            ResultSet foreignKeysOfForeignTable = dbMetaData.getImportedKeys(schema, schema, foreignTableName);
                            LinkedList<String> foreignKeysOfForeignTableList = new LinkedList<String>();

                            while (foreignKeysOfForeignTable.next()) {

                                foreignKeysOfForeignTableList.add(foreignKeysOfForeignTable.getString("FKCOLUMN_NAME"));

                            }

                            //Jos viitatun taulun pääavaimen koko on 1, lisätään kaari

                            if (foreignKeysOfForeignTableList.size() < 2) {

                                //Kaareen lisätään käsitelävän taulun nimi, sen pääavain, vierastaulun nimi ja sen pääavain.

                                LinkedList<String> primaryKeysOfTableValues = new LinkedList<String>();
                                for (int i=0; i<primaryKeysOfTable.size(); i++) {
                                    primaryKeysOfTableValues.add(primaryKeyForeignKeyValues.getString(primaryKeysOfTable.get(i)));
                                }

                                foreignKeysOfTableValues.add(primaryKeyForeignKeyValues.getString(fkColumnName));

                                Edge edge = new Edge(false,"1toN", tableName, primaryKeysOfTableValues, foreignTableName, foreignKeysOfTableValues, schema);

                                for (Vertex vertex : vertexes) {

                                    String tableNameInVertex = vertex.property("tableName");

                                    LinkedList<String> primaryKeysOfTableValuesInVertex = vertex.property("primaryKeysOfTableValues");

                                    if(tableNameInVertex.equals(tableName) && primaryKeysOfTableValuesInVertex.contains(primaryKeysOfTableValuesInVertex)) {

                                    }

                                }

                                Vertex vertex = tinkerGraph.addVertex("schema",schema,"tableName",tableName,"primaryKeysOfTableValues", primaryKeysOfTableValues, "jsonArray", jsonArray);
                                vertexes.add(vertex);
                                //edge.print();

                                edges.add(edge);

                            }

                        }

                        LinkedList<Edge> edgesForTable1 = edgesOfTable.get(tableName);

                        if(edgesForTable1 == null) {
                            edgesOfTable.put(tableName, edges);

                        } else {
                            edgesForTable1.addAll(edges);
                            edgesOfTable.put(tableName, edgesForTable1);
                        }

                        LinkedList<Edge> edgesForTable2 = edgesOfTable.get(foreignTableName);

                        if(edgesForTable2 == null) {
                            edgesOfTable.put(foreignTableName, edges);

                        } else {
                            edgesForTable2.addAll(edges);
                            edgesOfTable.put(foreignTableName, edgesForTable2);
                        }

                    }

                } else {

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
                        //ResultSet relationTableValues = executeSQLQuery("SELECT * FROM " + schema + "." + tableName + " LIMIT 20");
                        ResultSet relationTableValues = executeSQLQuery("SELECT * FROM " + schema + "." + tableName);

                        LinkedList<Edge> edges = new LinkedList<Edge>();

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

                        LinkedList<Edge> edgesForTable1 = edgesOfTable.get(referencedTables.get(primaryKeysOfTable.get(0)));

                        if(edgesForTable1 == null) {
                            edgesOfTable.put(referencedTables.get(primaryKeysOfTable.get(0)), edges);

                        } else {
                            edgesForTable1.addAll(edges);
                            edgesOfTable.put(referencedTables.get(primaryKeysOfTable.get(0)), edgesForTable1);
                        }

                        LinkedList<Edge> edgesForTable2 = edgesOfTable.get(referencedTables.get(primaryKeysOfTable.get(1)));

                        if(edgesForTable2 == null) {
                            edgesOfTable.put(referencedTables.get(primaryKeysOfTable.get(1)), edges);

                        } else {
                            edgesForTable2.addAll(edges);
                            edgesOfTable.put(referencedTables.get(primaryKeysOfTable.get(1)), edgesForTable2);
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

        System.out.println("Loading nodes");

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

                ResultSet foreignKeysOfForeignTable = dbMetaData.getImportedKeys(schema, schema, tableName);
                LinkedList<String> foreignKeysOfForeignTableList = new LinkedList<String>();

                while (foreignKeysOfForeignTable.next()) {

                    foreignKeysOfForeignTableList.add(foreignKeysOfForeignTable.getString("FKCOLUMN_NAME"));

                }

                boolean foreignKeysArePrimaryKeys = true;

                if(foreignKeysOfForeignTableList.size() > 0) {

                    for (String fkColumnName : foreignKeysOfForeignTableList) {

                        if (!primaryKeysOfTable.contains(fkColumnName)) {
                            foreignKeysArePrimaryKeys = false;
                        }

                    }

                } else {

                    foreignKeysArePrimaryKeys = false;

                }

                if(!foreignKeysArePrimaryKeys) {

                    String primaryKeys = String.join(",", primaryKeysOfTable);

                    ResultSet resultSet = executeSQLQuery("SELECT * FROM " + schema + "." + tableName + " ORDER BY " + primaryKeys);

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

                        for (String primaryKeyColumn : primaryKeysOfTable) {
                            primaryKeysOfTableValues.add(resultSet.getString(primaryKeyColumn));
                        }

                        Node node = new Node(schema, tableName, primaryKeysOfTableValues, jsonArray);

                        Vertex vertex = tinkerGraph.addVertex("schema",schema,"tableName",tableName,"primaryKeysOfTableValues", primaryKeysOfTableValues, "jsonArray", jsonArray);
                        vertexes.add(vertex);

                    }

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

        if (nodes != null) {

            for (Node node : nodes) {

                LinkedList<Edge> edges = edgesOfTable.get(node.getTableName());

                if(edges != null) {

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

            }

        } else {
            System.out.println("You must load edges and nodes first!");
        }

    }

    public void printEdges() {

        for (String table: edgesOfTable.keySet()){

            System.out.println("Table: " + table);

            LinkedList<Edge> edges = edgesOfTable.get(table);

            for (Edge edge : edges) {
                edge.print();
            }

        }

    }

    public void printNodes() {

        for (Node node : nodes) {
            node.print();
        }

    }

    public void printGraph() {

        for (Node node : nodes) {
            node.print();
        }

    }
}
