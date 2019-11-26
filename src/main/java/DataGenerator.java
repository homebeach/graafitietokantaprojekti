import com.github.javafaker.Faker;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

public class DataGenerator {

    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DB_URL = "jdbc:mariadb://127.0.0.1/";

    //  Database credentials
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    private int iterationsPerThread = 0;
    private int customerFactor = 0;
    private int invoiceFactor = 0;
    private int targetFactor = 0;
    private int workFactor = 0;
    private int itemFactor = 0;
    private int sequentialInvoices = 0;


    private List<String> firstnames;
    private List<String> surnames;
    private List<HashMap<String, String>> addresses;

    public ResultSet executeSQLInsert(String sqlQuery) {

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        try {

            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
            stmt = conn.createStatement();

            resultSet = stmt.executeQuery(sqlQuery);


        } catch (SQLException e) {
            System.out.println("SQLException");
            e.printStackTrace();
        } catch (Exception e) {
            System.out.println("Exception");
            e.printStackTrace();
        } finally {

            try {
                if (stmt != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                System.out.println("SQLException");
                se.printStackTrace();
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                System.out.println("SQLException");
                se.printStackTrace();
            }
        }

        return resultSet;
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

    public ResultSet truncateDatabase() {

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        try {

            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
            stmt = conn.createStatement();

            stmt.addBatch("SET FOREIGN_KEY_CHECKS=0;");
            stmt.addBatch("TRUNCATE TABLE warehouse.customer;");
            stmt.addBatch("TRUNCATE TABLE warehouse.invoice;");
            stmt.addBatch("TRUNCATE TABLE warehouse.target;");
            stmt.addBatch("TRUNCATE TABLE warehouse.useditem;");
            stmt.addBatch("TRUNCATE TABLE warehouse.warehouseitem;");
            stmt.addBatch("TRUNCATE TABLE warehouse.work;");
            stmt.addBatch("TRUNCATE TABLE warehouse.workhours;");
            stmt.addBatch("TRUNCATE TABLE warehouse.workinvoice;");
            stmt.addBatch("TRUNCATE TABLE warehouse.worktarget;");
            stmt.addBatch("TRUNCATE TABLE warehouse.worktype;");
            stmt.addBatch("SET FOREIGN_KEY_CHECKS=1;");
            stmt.executeBatch();

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

    public void getSampleData() {

        try {

            firstnames = new ArrayList<String>();
            surnames = new ArrayList<String>();
            addresses = new ArrayList<HashMap<String, String>>();

            ResultSet rs = executeSQLQuery("SELECT name FROM testdata.firstnames;");
            while (rs.next()) {
                String firstName = rs.getString("name");
                firstnames.add(firstName);
            }

            rs = executeSQLQuery("SELECT name FROM testdata.surnames;");
            while (rs.next()) {
                String surName = rs.getString("name");
                surnames.add(surName);
            }

            rs = executeSQLQuery("SELECT street, city, district, region, postcode FROM testdata.addresses;");
            while (rs.next()) {

                HashMap<String, String> address = new HashMap<String, String>();

                address.put("street", rs.getString("street"));
                address.put("city", rs.getString("city"));
                address.put("district", rs.getString("district"));
                address.put("region", rs.getString("region"));
                address.put("postcode", rs.getString("postcode"));

                addresses.add(address);
            }


        } catch (Exception e) {
            System.err.println("Exception: "
                    + e.getMessage());
        }

    }

    public void printSampleDataSizes() {

        System.out.println("Firstnames size: " + firstnames.size());
        System.out.println("Surnames size: " + surnames.size());
        System.out.println("Addresses size: " + addresses.size());
    }

    enum worktype {
        work, design, supporting_work
    }

    class RandomEnum<E extends Enum<worktype>> {
        Random r = new Random();
        E[] values;

        public RandomEnum(Class<E> token) {
            values = token.getEnumConstants();
        }

        public E random() {
            return values[r.nextInt(values.length)];
        }
    }


    public void insertData(int threadCount, int iterationsPerThread, int batchExecuteValue, int customerFactor, int invoiceFactor, int sequentialInvoices, int targetFactor, int workFactor, int itemFactor) {

        this.iterationsPerThread = iterationsPerThread;
        this.customerFactor = customerFactor;
        this.invoiceFactor = invoiceFactor;
        this.sequentialInvoices = sequentialInvoices;
        this.targetFactor = targetFactor;
        this.workFactor = workFactor;
        this.itemFactor = itemFactor;

        try {

            org.neo4j.driver.v1.Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "admin"));

            Session session = driver.session();

            truncateDatabase();

            session.run("MATCH (n) DETACH DELETE n");

            executeSQLInsert("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (0, 'MMJ 3X2,5MM² CABLE', 100, 'm', 0.64, 24, false)");
            executeSQLInsert("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (1, 'SOCKET 2-GROUND OL JUSSI', 20, 'pcs', 17.90, 24, false)");
            executeSQLInsert("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (2, 'SOCKET CORNER MODEL 3-PARTS', 10, 'pcs', 14.90, 24, false)");
            executeSQLInsert("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (3, 'COVER PLATE 2-OS JUSSI', 20, 'pcs', 3.90, 24, false)");
            executeSQLInsert("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (4, 'COVER PLATE 1-OS JUSSI', 20, 'pcs', 2.90, 24, false)");
            executeSQLInsert("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (5, 'SWITCH 5-SRJ SUBMERGED JUSSI', 25, 'pcs', 11.90, 24, false)");
            executeSQLInsert("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (6, 'SWITCH SURFACE JUSSI 1/6', 10, 'pcs', 8.90, 24, false)");
            executeSQLInsert("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (7, 'SHALLOW RVP 5-SWITCH', 5, 'pcs', 3.90, 24, false)");
            executeSQLInsert("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (8, 'BINDING SPIRAL 7,5-60MM INVISIBLE', 100, 'm', 0.09, 24, false)");

            session.run("CREATE (v:warehouseitem {warehouseitemId: 0, name:\"MMJ 3X2,5MM² CABLE\", balance:100, unit:\"m\", purchaseprice:0.64, vat:24, removed:\"false\"})");
            session.run("CREATE (v:warehouseitem {warehouseitemid: 1, name:\"SOCKET 2-GROUND OL JUSSI\", balance:20, unit:\"pcs\", purchaseprice:17.90, vat:24, removed:\"false\"})");
            session.run("CREATE (v:warehouseitem {warehouseitemid: 2, name:\"SOCKET CORNER MODEL 3-PARTS\", balance:10, unit:\"pcs\", purchaseprice:14.90, vat:24, removed:\"false\"})");
            session.run("CREATE (v:warehouseitem {warehouseitemid: 3, name:\"COVER PLATE 2-OS JUSSI\", balance:20, unit:\"pcs\", purchaseprice:3.90, vat:24, removed:\"false\"})");
            session.run("CREATE (v:warehouseitem {warehouseitemid: 4, name:\"COVER PLATE 1-OS JUSSI\", balance:20, unit:\"pcs\", purchaseprice:2.90, vat:24, removed:\"false\"})");
            session.run("CREATE (v:warehouseitem {warehouseitemid: 5, name:\"SWITCH 5-SRJ SUBMERGED JUSSI\", balance:25, unit:\"pcs\", purchaseprice:11.90, vat:24, removed:\"false\"})");
            session.run("CREATE (v:warehouseitem {warehouseitemid: 6, name:\"SWITCH SURFACE JUSSI 1/6\", balance:10, unit:\"pcs\", purchaseprice:8.90, vat:24, removed:\"false\"})");
            session.run("CREATE (v:warehouseitem {warehouseitemid: 7, name:\"SHALLOW RVP 5-SWITCH\", balance:5, unit:\"pcs\", purchaseprice:3.90, vat:24, removed:\"false\"})");
            session.run("CREATE (v:warehouseitem {warehouseitemid: 8, name:\"BINDING SPIRAL 7,5-60MM INVISIBLE\", balance:100, unit:\"m\", purchaseprice:0.09, vat:24, removed:\"false\"})");

            executeSQLInsert("INSERT INTO warehouse.worktype (id, name, price) VALUES (0, 'design', 55)");
            executeSQLInsert("INSERT INTO warehouse.worktype (id, name, price) VALUES (1, 'work', 45)");
            executeSQLInsert("INSERT INTO warehouse.worktype (id, name, price) VALUES (2, 'supporting work', 35)");

            session.run("CREATE (wt:worktype {worktypeId: 0, name:\"design\", price:55})");
            session.run("CREATE (wt:worktype {worktypeId: 1, name:\"work\", price:46})");
            session.run("CREATE (wt:worktype {worktypeId: 2, name:\"supporting work\", price:35})");

            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;

            Class.forName(JDBC_DRIVER);

            try (Connection connection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD)) {

                PreparedStatement customer = connection.prepareStatement("INSERT INTO warehouse.customer (id, name, address) VALUES (?,?,?)");
                PreparedStatement invoice = connection.prepareStatement("INSERT INTO warehouse.invoice (id, customerId, state, duedate, previousinvoice) VALUES (?,?,?,?,?)");
                PreparedStatement target = connection.prepareStatement("INSERT INTO warehouse.target (id, name, address, customerid) VALUES (?,?,?,?)");
                PreparedStatement work = connection.prepareStatement("INSERT INTO warehouse.work (id, name) VALUES (?,?)");

                PreparedStatement workTarget = connection.prepareStatement("INSERT INTO warehouse.worktarget (workId, targetId) VALUES (?,?)");

                PreparedStatement workInvoice = connection.prepareStatement("INSERT INTO warehouse.workinvoice (workId, invoiceId) VALUES (?,?)");
                PreparedStatement usedItem = connection.prepareStatement("INSERT INTO warehouse.useditem (amount, discount, workId, warehouseitemId) VALUES(?,?,?,?)");
                PreparedStatement workHours = connection.prepareStatement("INSERT INTO warehouse.workhours (worktypeId, hours, discount, workId) VALUES(?,?,?,?)");

                HashMap<String, PreparedStatement> preparedStatements = new HashMap<String, PreparedStatement>();

                preparedStatements.put("customer",customer);
                preparedStatements.put("invoice",invoice);
                preparedStatements.put("target",target);
                preparedStatements.put("work",work);
                preparedStatements.put("worktarget",workTarget);
                preparedStatements.put("workinvoice",workInvoice);
                preparedStatements.put("useditem",usedItem);
                preparedStatements.put("workhours",workHours);

                int customerIndex = 0;
                int invoiceIndex = 0;
                int targetIndex = 0;
                int workIndex = 0;
                int firstnameindex = 0;
                int surnameindex = 0;
                int addressindex = 0;

                for (int i = 0; i < threadCount; i++) {

                    DataGeneratorThread thread = new DataGeneratorThread(i, iterationsPerThread, batchExecuteValue, customerFactor, invoiceFactor, targetFactor, workFactor, itemFactor, sequentialInvoices, firstnames, surnames, addresses, customerIndex, invoiceIndex, targetIndex, workIndex);
                    thread.start();
                    customerIndex = customerIndex + iterationsPerThread*customerFactor;
                    invoiceIndex = invoiceIndex + iterationsPerThread*customerFactor*invoiceFactor;
                    targetIndex = targetIndex + iterationsPerThread*customerFactor*targetFactor;
                    workIndex = workIndex + iterationsPerThread*workFactor;

                }

            }

            session.close();
            driver.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}