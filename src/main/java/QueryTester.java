
import org.neo4j.driver.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class QueryTester {

    private HashMap<String, String[]> sql_databases;

    private HashMap<String, String> neo4j_settings;

    private HashMap<String, ArrayList<Long>> resultLists;

    private List<Long> results;

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


            System.out.println("Starting iteration: " + i + ".");

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
            System.out.println("Smallest number in resultset: ");
            System.out.println(results.get(0));
            System.out.println("Biggest number in resultset: ");
            System.out.println(results.get(results.size() - 1));
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
                System.out.println(results.get(i));
            }
            sum = sum + results.get(i);
        }

        double average = sum / results.size();

        double standardDeviation = calculateStandardDeviation(results);

        if(showAll) {
            System.out.println();
        }

        System.out.println("Average time for query: ");
        System.out.println(average);
        System.out.println();

        System.out.println("Standard deviation of the results array: ");
        System.out.println(standardDeviation);
        System.out.println();



    }

    public static double calculateStandardDeviation(List<Long> results)
    {
        double sum = 0.0, standardDeviation = 0.0;
        int size = results.size();

        for(long result : results) {
            sum += result;
        }

        double mean = sum/size;

        for(double result: results) {
            standardDeviation += Math.pow(result - mean, 2);
        }

        return Math.sqrt(standardDeviation/size);
    }

    public void executeQueryTests(int iterations, boolean showAll) {

        System.out.println("Short query, worktype price");

        String workItemPriceSQL = "SELECT (price * hours * workhours.discount) as price FROM worktype,workhours,work WHERE worktype.id=workhours.worktypeId AND workhours.workId=work.id;";

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

        String workItemPriceCypher = "MATCH (wt:worktype)-[h:WORKHOURS]->(w:work) RETURN (h.hours*h.discount*wt.price) as price;";

        results = measureQueryTimeCypher(workItemPriceCypher, iterations);

        showResults(results, showAll);

        System.out.println();

        System.out.println("Long query, work price");

        String workPriceSQL = "SELECT (price * hours * workhours.discount) + (purchaseprice * amount * useditem.discount) as price FROM worktype,workhours,work,useditem,item WHERE worktype.id=workhours.worktypeId AND workhours.workId=work.id AND work.id=useditem.workId AND useditem.itemId=item.id";

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

        String workPriceCypher = "MATCH (wt:worktype)-[h:WORKHOURS]->(w:work)-[u:USED_ITEM]->(i:item) RETURN (h.hours*h.discount*wt.price)+(u.amount*u.discount*i.purchaseprice) as price";

        results = measureQueryTimeCypher(workPriceCypher, iterations);

        showResults(results, showAll);

        System.out.println();

        System.out.println("Query with defined key, work of invoice");

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

    public void executeAggregateQueryTest(int iterations, boolean showAll) {

        System.out.println("Aggregate query, invoice price");

        String invoicePriceSQL = "SELECT q1.invoiceId AS invoiceId, sum(q2.price) AS invoicePrice " +
                "FROM (" +
                "SELECT invoice.id AS invoiceId, work.id AS workId " +
                "FROM invoice, workinvoice, work " +
                "WHERE invoice.id=workinvoice.invoiceId and workinvoice.workId=work.id " +
                ") AS q1, (" +
                "SELECT work.id AS workId, SUM((worktype.price * workhours.hours * workhours.discount) + (item.purchaseprice * useditem.amount * useditem.discount)) AS price " +
                "FROM worktype,workhours,work,item,useditem " +
                "WHERE worktype.id=workhours.worktypeid AND workhours.workid=work.id AND work.id=useditem.workid AND useditem.itemid=item.id GROUP BY work.id" +
                ") AS q2 " +
                "WHERE q1.workId = q2.workId GROUP BY q1.invoiceId";

        resultLists = measureQueryTimeSQL(invoicePriceSQL, iterations);

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

        String invoicePriceCypher = "MATCH (inv:invoice)-[:WORK_INVOICE]->(w:work)<-[h:WORKHOURS]-(wt:worktype) " +
                "WITH inv, w, SUM(wt.price*h.hours*h.discount) as workTimePrice " +
                "OPTIONAL MATCH " +
                "(w)-[u:USED_ITEM]->(i:item) " +
                "WITH inv, workTimePrice + SUM(u.amount*u.discount*i.purchaseprice) as workItemPrice " +
                "RETURN inv, sum(workItemPrice) as invoicePrice";

        results = measureQueryTimeCypher(invoicePriceCypher, iterations);

        showResults(results, showAll);

        System.out.println();
    }


    public void executeRecursiveQueryTest(int iterations, boolean showAll, int invoiceId) {

        System.out.println("Recursive query, invoices related to invoice id " + invoiceId);

        String previousInvoicesSQL = "SELECT  id,customerid,state,duedate,previousinvoice " +
                "FROM (SELECT * FROM invoice " +
                "ORDER BY previousinvoice, id) invoices_sorted, " +
                "(SELECT @pv := '" + invoiceId + "') initialisation " +
                "WHERE find_in_set(previousinvoice, @pv) " +
                "AND length(@pv := concat(@pv, ',', id))";

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