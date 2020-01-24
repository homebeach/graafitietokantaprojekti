
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.neo4j.driver.Values.parameters;

public class QueryTester {

    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DB_URL = "jdbc:mariadb://127.0.0.1/";
    private static final String DATABASE = "warehouse";

    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    public void measureQueryTimeSQL(Statement stmt, String sqlQuery) throws SQLException {

        System.out.println("Executing SQL Query: " + sqlQuery);

        long startTimeInMilliseconds = System.currentTimeMillis();

        ResultSet resultSet = stmt.executeQuery(sqlQuery);

        long endTimeInMilliseconds = System.currentTimeMillis();
        long elapsedTimeMilliseconds = endTimeInMilliseconds - startTimeInMilliseconds;
        //String elapsedTime = (new SimpleDateFormat("mm:ss:SSS")).format(new Date(elapsedTimeMilliseconds));
        //System.out.println("Time elapsed: " + elapsedTime);
        System.out.println("Time elapsed in milliseconds: " + elapsedTimeMilliseconds);

    }

    public void measureQueryTimeCypher(Session session, String cypherQuery) throws SQLException {

        System.out.println("Executing Cypher Query: " + cypherQuery);

        long startTimeInMilliseconds = System.currentTimeMillis();

        Result result = session.readTransaction(tx -> tx.run(cypherQuery));

        long endTimeInMilliseconds = System.currentTimeMillis();
        long elapsedTimeMilliseconds = endTimeInMilliseconds - startTimeInMilliseconds;
        ///String elapsedTime = (new SimpleDateFormat("mm:ss:SSS")).format(new Date(elapsedTimeMilliseconds));
        //System.out.println("Time elapsed: " + elapsedTimeMilliseconds);
        System.out.println("Time elapsed in milliseconds: " + elapsedTimeMilliseconds);


    }

    public void executeQueryTests() {

        org.neo4j.driver.Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "admin"));

        Session session = driver.session();

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        try {

            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(DB_URL + DATABASE, USERNAME, PASSWORD);
            stmt = conn.createStatement();

            String workItemPriceSQL = "SELECT (purchaseprice * amount * useditem.discount) AS price FROM work,warehouseitem,useditem " +
                    "WHERE work.id=useditem.workid AND warehouseitem.id=useditem.warehouseitemid";

            measureQueryTimeSQL(stmt, workItemPriceSQL);

            String workItemPriceCypher = "MATCH (w:work)-[u:USED_ITEM]->(i:warehouseitem) RETURN u.amount*u.discount*i.purchaseprice";

            measureQueryTimeCypher(session, workItemPriceCypher);

            System.out.println();

            String workPriceSQL = "SELECT (price * hours * workhours.discount) + (purchaseprice * amount * useditem.discount) as price FROM worktype,workhours,work,warehouseitem,useditem WHERE worktype.id=workhours.worktypeid AND workhours.workid=work.id AND work.id=useditem.workid AND warehouseitem.id=useditem.warehouseitemid";

            measureQueryTimeSQL(stmt, workPriceSQL);

            String workPriceCypher = "MATCH (wt:worktype)-[h:WORKHOURS]->(w:work)-[u:USED_ITEM]->(i:warehouseitem) RETURN (h.hours*h.discount*wt.price)+(u.amount*u.discount*i.purchaseprice)";

            measureQueryTimeCypher(session, workPriceCypher);

            System.out.println();

            //asiakkaan laskujen töiden summat

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

            measureQueryTimeSQL(stmt, previousInvoicesSQL);

            String previousInvoicesCypher = "MATCH (i:invoice)-[p:PREVIOUS_INVOICE *0..]->(j:invoice) RETURN *";

            measureQueryTimeCypher(session, previousInvoicesCypher);


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