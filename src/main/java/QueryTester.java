
import org.neo4j.driver.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryTester {

    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DB_URL = "jdbc:mariadb://127.0.0.1/";
    private static final String DATABASE = "warehouse";

    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    public List<Long> measureQueryTimeSQL(Statement stmt, String sqlQuery, int iterations) throws SQLException {

        System.out.println("Executing SQL Query: " + sqlQuery + " with " + iterations + " iterations.");

        List<Long> results = new ArrayList<Long>();

        ResultSet resultSet = null;

        for(int i=0; i<iterations; i++) {

            long startTimeInMilliseconds = System.currentTimeMillis();

            resultSet = stmt.executeQuery(sqlQuery);

            long endTimeInMilliseconds = System.currentTimeMillis();
            long elapsedTimeMilliseconds = endTimeInMilliseconds - startTimeInMilliseconds;

            results.add(elapsedTimeMilliseconds);

        }

        resultSet.last();
        System.out.println("Query returned "+resultSet.getRow()+" rows.");

        return results;
    }

    public List<Long> measureQueryTimeCypher(Session session, String cypherQuery, int iterations) throws SQLException {

        System.out.println("Executing Cypher Query: " + cypherQuery + " with " + iterations + " iterations.");

        List<Long> results = new ArrayList<Long>();

        Result result = null;

        for(int i=0; i<iterations; i++) {

            long startTimeInMilliseconds = System.currentTimeMillis();

            //Result result = session.readTransaction(tx -> tx.run(cypherQuery));

            result = session.run(cypherQuery);

            long endTimeInMilliseconds = System.currentTimeMillis();
            long elapsedTimeMilliseconds = endTimeInMilliseconds - startTimeInMilliseconds;

            results.add(elapsedTimeMilliseconds);

        }

        List<Record> records = result.list();

        System.out.println("Query returned "+ records.size() +" records.");

        return results;
    }

    public void showResults(List<Long> results, boolean showAll) throws SQLException {


        if(showAll) {
            System.out.println("Smallest number in resultset: " + results.get(0) + ".");
            System.out.println("Biggest number in resultset: " + results.get(results.size()) + ".");
        }

        Collections.sort(results);
        results.remove(0);
        results.remove(results.size() -1);

        long sum = 0;

        for(int i=0; i<results.size(); i++) {

            if(showAll) {
                System.out.println("Result with index " + i + ": " + results.get(0) + ".");
            }
            sum = sum + results.get(i);
        }

        double average = sum / results.size();

        System.out.println("Average time for query" + average);


    }

    public void executeQueryTests(int iterations) {

        org.neo4j.driver.Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "admin"));

        Session session = driver.session();

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        boolean showAll = true;

        try {

            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(DB_URL + DATABASE, USERNAME, PASSWORD);
            stmt = conn.createStatement();

            String workItemPriceSQL = "SELECT (purchaseprice * amount * useditem.discount) AS price FROM work,warehouseitem,useditem " +
                    "WHERE work.id=useditem.workid AND warehouseitem.id=useditem.warehouseitemid";

            List<Long> results = measureQueryTimeSQL(stmt, workItemPriceSQL, iterations);

            showResults(results, showAll);

            String workItemPriceCypher = "MATCH (w:work)-[u:USED_ITEM]->(i:warehouseitem) RETURN u.amount*u.discount*i.purchaseprice";

            results = measureQueryTimeCypher(session, workItemPriceCypher, iterations);

            showResults(results, showAll);

            System.out.println();

            String workPriceSQL = "SELECT (price * hours * workhours.discount) + (purchaseprice * amount * useditem.discount) as price FROM worktype,workhours,work,warehouseitem,useditem WHERE worktype.id=workhours.worktypeid AND workhours.workid=work.id AND work.id=useditem.workid AND warehouseitem.id=useditem.warehouseitemid";

            results = measureQueryTimeSQL(stmt, workPriceSQL, iterations);

            showResults(results, showAll);

            String workPriceCypher = "MATCH (wt:worktype)-[h:WORKHOURS]->(w:work)-[u:USED_ITEM]->(i:warehouseitem) RETURN (h.hours*h.discount*wt.price)+(u.amount*u.discount*i.purchaseprice)";

            results = measureQueryTimeCypher(session, workPriceCypher, iterations);

            showResults(results, showAll);

            System.out.println();

            //asiakkaan laskujen töiden summat

            /*

            String customerWorkPricesSQL = "SELECT q1.customerId, q1.invoiceId, q2.workId, q2.price " +
            "FROM (SELECT customer.id AS customerId, invoice.id AS invoiceId, work.id AS workId FROM customer, invoice, workinvoice, work " +
            "WHERE customer.id=invoice.customerid AND invoice.id=workinvoice.invoiceId AND workinvoice.workId=work.id) AS q1, " +
            "(SELECT work.id AS workId, SUM((price * hours * workhours.discount) + (purchaseprice * amount * useditem.discount)) AS price " +
            "FROM worktype,workhours,work,warehouseitem,useditem WHERE worktype.id=workhours.worktypeid AND workhours.workid=work.id AND work.id=useditem.workid AND useditem.warehouseitemid=warehouseitem.id GROUP BY work.id) AS q2 " +
            "WHERE q1.workId = q2.workId";

            measureQueryTimeSQL(stmt, customerWorkPricesSQL);

            String customerWorkPricesCypher = "MATCH (c:customer)-[p:PAYS]->(i:invoice)-[wi:WORK_INVOICE]->(w:work) " +
            "MATCH (wt:worktype)-[h:WORKHOURS]->(w:work)-[u:USED_ITEM]->(i:warehouseitem) " +
            "RETURN  c.customerId, i.invoiceId,w.workId, sum((h.hours*h.discount*wt.price)+(u.amount*u.discount*i.purchaseprice))";

            measureQueryTimeCypher(session, customerWorkPricesCypher);


             */
            System.out.println();

            String previousInvoicesSQL = "WITH previous_invoices AS (" +
                    "SELECT id,customerId,state,duedate,previousinvoice " +
                    "FROM invoice " +
                    "UNION ALL " +
                    "SELECT j.id, j.customerId,j.state,j.duedate,j.previousinvoice " +
                    "FROM invoice i, invoice j " +
                    "WHERE i.id = j.previousinvoice " +
                    ") " +
                    "SELECT * FROM previous_invoices";

            results = measureQueryTimeSQL(stmt, previousInvoicesSQL, iterations);

            showResults(results, showAll);

            String previousInvoicesCypher = "MATCH (i:invoice)-[p:PREVIOUS_INVOICE *0..]->(j:invoice) RETURN *";

            results = measureQueryTimeCypher(session, previousInvoicesCypher, iterations);

            showResults(results, showAll);


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

        session.close();
        driver.close();

/*
        Tässä rekursiiviset esimerkkikyselyt:

        MariaDB:

        WITH previous_invoices AS (
                SELECT
                id,
                customerId,
                state,
                duedate,
                previousinvoice
                FROM
                invoice
                WHERE id=0
                UNION ALL
                SELECT
                j.id,
                j.customerId,
                j.state,
                j.duedate,
                j.previousinvoice
                FROM
                invoice i, invoice j
                WHERE i.id = j.previousinvoice
        )
        SELECT
                *
                FROM
        previous_invoices;

        Neo4J

        MATCH (i:invoice { invoiceId:0 })-[p:PREVIOUS_INVOICE *0..]->(j:invoice)
        RETURN *;





        MariaDB:

SELECT q1.customerId, q1.invoiceId, q2.workId, q2.price
FROM (select customer.id as customerId, invoice.id as invoiceId, work.id as workId from customer, invoice, workinvoice, work where customer.id=invoice.customerid and invoice.id=workinvoice.invoiceId and workinvoice.workId=work.id
) as q1, (select work.id as workId, sum((price * hours * workhours.discount) + (purchaseprice * amount * useditem.discount)) as price from worktype,workhours,work,warehouseitem,useditem where worktype.id=workhours.worktypeid and workhours.workid=work.id and work.id=useditem.workid and useditem.warehouseitemid=warehouseitem.id group by work.id
) as q2
WHERE q1.workId = q2.workId;

Cypher:

MATCH (c:customer)-[p:PAYS]->(i:invoice)-[wi:WORK_INVOICE]->(w:work)
MATCH (wt:worktype)-[h:WORKHOURS]->(w:work)-[u:USED_ITEM]->(i:warehouseitem)
RETURN  c.customerId, i.invoiceId,w.workId, sum((h.hours*h.discount*wt.price)+(u.amount*u.discount*i.purchaseprice));




Lyhyt kysely:

        Neo4J:

        MATCH (w:work)-[u:USED_ITEM]->(i:warehouseitem) RETURN u.amount*u.discount*i.purchaseprice;

        Started streaming 100000 records after 6 ms and completed after 703 ms, displaying first 1000 rows.

        MariaDB:

        select (purchaseprice * amount * useditem.discount) as price from work,warehouseitem,useditem
        where work.id=useditem.workid and warehouseitem.id=useditem.warehouseitemid;


        Pitkä kysely:

        Neo4J:

        MATCH (wt:worktype)-[h:WORKHOURS]->(w:work)-[u:USED_ITEM]->(i:warehouseitem) RETURN (h.hours*h.discount*wt.price)+(u.amount*u.discount*i.purchaseprice);
        Started streaming 300000 records after 29 ms and completed after 3437 ms, displaying first 1000 rows.


                MariaDB:

        select (price * hours * workhours.discount) + (purchaseprice * amount * useditem.discount) as price from worktype,workhours,work,warehouseitem,useditem where worktype.id=workhours.worktypeid and workhours.workid=work.id and work.id=useditem.workid and warehouseitem.id=useditem.warehouseitemid;




        */

    }

}