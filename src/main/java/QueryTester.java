
import org.neo4j.driver.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class QueryTester {

    private HashMap<String, String[]> sql_databases;

    HashMap<String, String> neo4j_settings;

    public QueryTester(HashMap<String, String[]> sql_databases, HashMap<String, String> neo4j_settings) {
        this.sql_databases = sql_databases;
        this.neo4j_settings = neo4j_settings;
    }

    public HashMap<String, ArrayList<Long>> measureQueryTimeSQL(String sqlQuery, int iterations) {

        HashMap<String, ArrayList<Long>> resultLists = new HashMap<String, ArrayList<Long>>();

        ArrayList<Long> results = null;

        Connection connection = null;
        Statement stmt = null;

        System.out.println("Executing SQL Query: " + sqlQuery + " in " + sql_databases.size() + " databases with " + iterations + " iterations.");

        try {

            for (String db_url : sql_databases.keySet()) {

                String[] db_info = sql_databases.get(db_url);

                String db_driver = db_info[0];
                String db_username = db_info[1];
                String db_password = db_info[2];

                Class.forName(db_driver);

                connection = DriverManager.getConnection(db_url + "warehouse", db_username, db_password);

                DatabaseMetaData meta = connection.getMetaData();

                String productName = meta.getDatabaseProductName();
                String productVersion = meta.getDatabaseProductVersion();

                stmt = connection.createStatement();

                results = new ArrayList<Long>();

                ResultSet resultSet = null;

                for(int i=0; i<iterations; i++) {

                    long startTimeInMilliseconds = System.currentTimeMillis();

                    resultSet = stmt.executeQuery(sqlQuery);

                    long endTimeInMilliseconds = System.currentTimeMillis();
                    long elapsedTimeMilliseconds = endTimeInMilliseconds - startTimeInMilliseconds;

                    results.add(elapsedTimeMilliseconds);

                }

                resultLists.put(productVersion, results);

                resultSet.last();
                System.out.println("Query in url " + db_url + " returned " + resultSet.getRow() + " rows.");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                if (stmt != null) {
                    connection.close();
                }
            } catch (SQLException se) {
            }
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }

        System.out.println();

        return resultLists;
    }

    public List<Long> measureQueryTimeCypher(String cypherQuery, int iterations) {

        String neo4j_db_url = neo4j_settings.get("NEO4J_DB_URL");
        String neo4j_username = neo4j_settings.get("NEO4J_USERNAME");
        String neo4j_password = neo4j_settings.get("NEO4J_PASSWORD");

        org.neo4j.driver.Driver driver = GraphDatabase.driver(neo4j_db_url, AuthTokens.basic(neo4j_username, neo4j_password));

        Session session = driver.session();

        List<Long> results = new ArrayList<Long>();

        Result result = null;

        System.out.println("Executing Cypher Query: " + cypherQuery + " with " + iterations + " iterations.");

        for(int i=0; i<iterations; i++) {

            long startTimeInMilliseconds = System.currentTimeMillis();

            //Result result = session.readTransaction(tx -> tx.run(cypherQuery));

            result = session.run(cypherQuery);

            long endTimeInMilliseconds = System.currentTimeMillis();
            long elapsedTimeMilliseconds = endTimeInMilliseconds - startTimeInMilliseconds;

            results.add(elapsedTimeMilliseconds);

        }

        List<Record> records = result.list();

        System.out.println("Cypher query returned: " + records.size() + " records.");

        session.close();
        driver.close();

        return results;
    }

    public void showResults(List<Long> results, boolean showAll) {

        Collections.sort(results);

        if(showAll) {
            System.out.println("Smallest number in resultset: " + results.get(0) + " ms.");
            System.out.println("Biggest number in resultset: " + results.get(results.size() - 1) + " ms.");
        }

        if(results.size() > 2) {
            results.remove(0);
            results.remove(results.size() - 1);
        }

        long sum = 0;

        if(showAll) {
            System.out.println();
            System.out.println("Content of the results table:");
        }

        for(int i=0; i<results.size(); i++) {

            if(showAll) {
                System.out.println("Index " + i + ": " + results.get(i) + ".");
            }
            sum = sum + results.get(i);
        }

        double average = sum / results.size();

        if(showAll) {
            System.out.println();
        }

        System.out.println("Average time for query: " + average + " ms.");
        System.out.println();


    }

    public void showSystemInfo(List<Long> results, boolean showAll) {


    }

    public void executeQueryTests(int iterations, boolean showAll) {

        HashMap<String, ArrayList<Long>> resultLists;
        List<Long> results;


        String workItemPriceSQL = "SELECT (purchaseprice * amount * useditem.discount) AS price FROM work,item,useditem " +
                    "WHERE work.id=useditem.workid AND item.id=useditem.itemid";

        resultLists = measureQueryTimeSQL(workItemPriceSQL, iterations);

        for (String databaseVersion : resultLists.keySet()) {

            if(databaseVersion.contains("MariaDB")) {
                System.out.println("Results for MariaDB version " + databaseVersion);
            }
            else {
                System.out.println("Results for MySQL version " + databaseVersion);
            }

            results = resultLists.get(databaseVersion);
            showResults(results, showAll);

        }

        System.out.println();

        String workItemPriceCypher = "MATCH (w:work)-[u:USED_ITEM]->(i:item) RETURN u.amount*u.discount*i.purchaseprice";

        results = measureQueryTimeCypher(workItemPriceCypher, iterations);

        showResults(results, showAll);

        System.out.println();

        String workPriceSQL = "SELECT (price * hours * workhours.discount) + (purchaseprice * amount * useditem.discount) as price FROM worktype,workhours,work,item,useditem WHERE worktype.id=workhours.worktypeid AND workhours.workid=work.id AND work.id=useditem.workid AND item.id=useditem.itemid";

        resultLists = measureQueryTimeSQL(workPriceSQL, iterations);

        for (String databaseVersion : resultLists.keySet()) {

            if(databaseVersion.contains("MariaDB")) {
                System.out.println("Results for MariaDB version " + databaseVersion);
            }
            else {
                System.out.println("Results for MySQL version " + databaseVersion);
            }

            results = resultLists.get(databaseVersion);
            showResults(results, showAll);

        }

        System.out.println();

        String workPriceCypher = "MATCH (wt:worktype)-[h:WORKHOURS]->(w:work)-[u:USED_ITEM]->(i:item) RETURN (h.hours*h.discount*wt.price)+(u.amount*u.discount*i.purchaseprice)";

        results = measureQueryTimeCypher(workPriceCypher, iterations);

        showResults(results, showAll);

        System.out.println();

        String workOfInvoiceSQL = "SELECT * FROM invoice,workinvoice,work " +
                "WHERE invoice.id=workinvoice.workId AND workinvoice.workId=work.id AND invoice.id=0";

        resultLists = measureQueryTimeSQL(workOfInvoiceSQL, iterations);

        for (String databaseVersion : resultLists.keySet()) {

            if(databaseVersion.contains("MariaDB")) {
                System.out.println("Results for MariaDB version " + databaseVersion);
            }
            else {
                System.out.println("Results for MySQL version " + databaseVersion);
            }

            results = resultLists.get(databaseVersion);
            showResults(results, showAll);

        }

        System.out.println();

        String workOfInvoiceCypher = "MATCH (i:invoice { invoiceId:0 })-[wi:WORK_INVOICE]->(w:work) RETURN *";

        results = measureQueryTimeCypher(workOfInvoiceCypher, iterations);

        showResults(results, showAll);

    }

    public void executeRecursiveQueryTest(int iterations, boolean showAll, int invoiceId) {

            HashMap<String, ArrayList<Long>> resultLists;
            List<Long> results;

            String previousInvoicesSQL = "WITH previous_invoices AS (" +
                    "SELECT id,customerId,state,duedate,previousinvoice " +
                    "FROM invoice " +
                    "WHERE id=" + invoiceId + " " +
                    "UNION ALL " +
                    "SELECT j.id, j.customerId,j.state,j.duedate,j.previousinvoice " +
                    "FROM invoice i, invoice j " +
                    "WHERE i.id = j.previousinvoice " +
                    ") " +
                    "SELECT * FROM previous_invoices";

            sql_databases.remove("jdbc:mysql://127.0.0.1:3307/");

            resultLists = measureQueryTimeSQL(previousInvoicesSQL, iterations);

            for (String databaseVersion : resultLists.keySet()) {

                if(databaseVersion.contains("MariaDB")) {
                    System.out.println("Results for MariaDB version " + databaseVersion);
                }
                else {
                    System.out.println("Results for MySQL version " + databaseVersion);
                }

                results = resultLists.get(databaseVersion);
                showResults(results, showAll);

            }


            String previousInvoicesCypher = "MATCH (i:invoice { invoiceId:" + invoiceId + " })-[p:PREVIOUS_INVOICE *0..]->(j:invoice) RETURN *";

            results = measureQueryTimeCypher(previousInvoicesCypher, iterations);

            showResults(results, showAll);

        }

}