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

    public void executeSQLUpdate(String sqlQuery) {

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        try {

            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
            stmt = conn.createStatement();

            stmt.executeUpdate(sqlQuery);


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

    }

    public void executeSQLUpdate(String sqlQuery, String db_url) {

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        try {

            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(db_url, USERNAME, PASSWORD);
            stmt = conn.createStatement();

            stmt.executeUpdate(sqlQuery);


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


    public void createBasicData(Session session) {

        executeSQLUpdate("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (0, 'MMJ 3X2,5MM² CABLE', 100, 'm', 0.64, 24, false)");
        executeSQLUpdate("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (1, 'SOCKET 2-GROUND OL JUSSI', 20, 'pcs', 17.90, 24, false)");
        executeSQLUpdate("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (2, 'SOCKET CORNER MODEL 3-PARTS', 10, 'pcs', 14.90, 24, false)");
        executeSQLUpdate("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (3, 'COVER PLATE 2-OS JUSSI', 20, 'pcs', 3.90, 24, false)");
        executeSQLUpdate("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (4, 'COVER PLATE 1-OS JUSSI', 20, 'pcs', 2.90, 24, false)");
        executeSQLUpdate("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (5, 'SWITCH 5-SRJ SUBMERGED JUSSI', 25, 'pcs', 11.90, 24, false)");
        executeSQLUpdate("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (6, 'SWITCH SURFACE JUSSI 1/6', 10, 'pcs', 8.90, 24, false)");
        executeSQLUpdate("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (7, 'SHALLOW RVP 5-SWITCH', 5, 'pcs', 3.90, 24, false)");
        executeSQLUpdate("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (8, 'BINDING SPIRAL 7,5-60MM INVISIBLE', 100, 'm', 0.09, 24, false)");


        session.run("CREATE (v:warehouseitem {warehouseitemId: 0, name:\"MMJ 3X2,5MM² CABLE\", balance:100, unit:\"m\", purchaseprice:0.64, vat:24, removed:\"false\"})");
        session.run("CREATE (v:warehouseitem {warehouseitemId: 1, name:\"SOCKET 2-GROUND OL JUSSI\", balance:20, unit:\"pcs\", purchaseprice:17.90, vat:24, removed:\"false\"})");
        session.run("CREATE (v:warehouseitem {warehouseitemId: 2, name:\"SOCKET CORNER MODEL 3-PARTS\", balance:10, unit:\"pcs\", purchaseprice:14.90, vat:24, removed:\"false\"})");
        session.run("CREATE (v:warehouseitem {warehouseitemId: 3, name:\"COVER PLATE 2-OS JUSSI\", balance:20, unit:\"pcs\", purchaseprice:3.90, vat:24, removed:\"false\"})");
        session.run("CREATE (v:warehouseitem {warehouseitemId: 4, name:\"COVER PLATE 1-OS JUSSI\", balance:20, unit:\"pcs\", purchaseprice:2.90, vat:24, removed:\"false\"})");
        session.run("CREATE (v:warehouseitem {warehouseitemId: 5, name:\"SWITCH 5-SRJ SUBMERGED JUSSI\", balance:25, unit:\"pcs\", purchaseprice:11.90, vat:24, removed:\"false\"})");
        session.run("CREATE (v:warehouseitem {warehouseitemId: 6, name:\"SWITCH SURFACE JUSSI 1/6\", balance:10, unit:\"pcs\", purchaseprice:8.90, vat:24, removed:\"false\"})");
        session.run("CREATE (v:warehouseitem {warehouseitemId: 7, name:\"SHALLOW RVP 5-SWITCH\", balance:5, unit:\"pcs\", purchaseprice:3.90, vat:24, removed:\"false\"})");
        session.run("CREATE (v:warehouseitem {warehouseitemId: 8, name:\"BINDING SPIRAL 7,5-60MM INVISIBLE\", balance:100, unit:\"m\", purchaseprice:0.09, vat:24, removed:\"false\"})");

        executeSQLUpdate("INSERT INTO warehouse.worktype (id, name, price) VALUES (0, 'design', 55)");
        executeSQLUpdate("INSERT INTO warehouse.worktype (id, name, price) VALUES (1, 'work', 45)");
        executeSQLUpdate("INSERT INTO warehouse.worktype (id, name, price) VALUES (2, 'supporting work', 35)");

        session.run("CREATE (wt:worktype {worktypeId: 0, name:\"design\", price:55})");
        session.run("CREATE (wt:worktype {worktypeId: 1, name:\"work\", price:46})");
        session.run("CREATE (wt:worktype {worktypeId: 2, name:\"supporting work\", price:35})");

    }


    public void createTables() {


        String database = "warehouse";

        String createWarehouse = "CREATE DATABASE IF NOT EXISTS `" + database + "`";

        //PreparedStatement customer = connection.prepareStatement("INSERT INTO warehouse.customer (id, name, address) VALUES (?,?,?)");

        String customer = "CREATE TABLE IF NOT EXISTS `customer` (" +
        "`id` bigint(20) unsigned NOT NULL," +
        "`name` varchar(50) NOT NULL CHECK (`name` <> '')," +
        "`address` varchar(150) NOT NULL CHECK (`address` <> '')," +
        "PRIMARY KEY (`id`))";

        //PreparedStatement usedItem = connection.prepareStatement("INSERT INTO warehouse.useditem (amount, discount, workId, warehouseitemId) VALUES(?,?,?,?)");

        String usedItem = "CREATE TABLE IF NOT EXISTS `useditem` (" +
        "`amount` int(11) DEFAULT NULL CHECK (`amount` > 0)," +
        "`discount` decimal(65,2) DEFAULT NULL," +
        "`workId` bigint(20) unsigned NOT NULL," +
        "`warehouseitemId` bigint(20) unsigned NOT NULL," +
        "PRIMARY KEY (`workId`,`warehouseitemId`)," +
        "KEY `warehouseitemId` (`warehouseitemId`)," +
        "CONSTRAINT `useditem_ibfk_1` FOREIGN KEY (`workId`) REFERENCES `work` (`id`)," +
        "CONSTRAINT `useditem_ibfk_2` FOREIGN KEY (`warehouseitemId`) REFERENCES `warehouseitem` (`id`))";

        //PreparedStatement invoice = connection.prepareStatement("INSERT INTO warehouse.invoice (id, customerId, state, duedate, previousinvoice) VALUES (?,?,?,?,?)");

        String invoice = "CREATE TABLE IF NOT EXISTS `invoice` (" +
        "`id` bigint(20) unsigned NOT NULL," +
        "`customerId` bigint(20) unsigned NOT NULL," +
        "`state` int(11) NOT NULL," +
        "`duedate` date DEFAULT NULL," +
        "`firstinvoice` bigint(20) unsigned NOT NULL," +
        "`previousinvoice` bigint(20) unsigned NOT NULL," +
        "PRIMARY KEY (`id`)," +
        "KEY `customerId` (`customerId`)," +
        "CONSTRAINT `customer_ibfk_1` FOREIGN KEY (`customerId`) REFERENCES `customer` (`id`))";

        //PreparedStatement workInvoice = connection.prepareStatement("INSERT INTO warehouse.workinvoice (workId, invoiceId) VALUES (?,?)");

        String workInvoice = "CREATE TABLE IF NOT EXISTS `workinvoice` (" +
                "`workId` bigint(20) unsigned NOT NULL," +
                "`invoiceId` bigint(20) unsigned NOT NULL," +
                "PRIMARY KEY (`workId`,`invoiceId`)," +
                "KEY `workId` (`workId`)," +
                "KEY `invoiceId` (`invoiceId`)," +
                "CONSTRAINT `workinvoice_ibfk_1` FOREIGN KEY (`workId`) REFERENCES `work` (`id`)," +
                "CONSTRAINT `workinvoice_ibfk_2` FOREIGN KEY (`invoiceId`) REFERENCES `invoice` (`id`))";


        //PreparedStatement work = connection.prepareStatement("INSERT INTO warehouse.work (id, name) VALUES (?,?)");


        String work = "CREATE TABLE IF NOT EXISTS `work` (" +
        "`id` bigint(20) unsigned NOT NULL," +
        "`type` int(11) NOT NULL," +
        "`price` decimal(65,2) DEFAULT NULL," +
        "`invoiceId` bigint(20) unsigned NOT NULL," +
        "`targetId` bigint(20) unsigned NOT NULL," +
        "PRIMARY KEY (`id`)," +
        "KEY `invoiceId` (`invoiceId`)," +
        "KEY `targetId` (`targetId`)," +
        "CONSTRAINT `work_ibfk_1` FOREIGN KEY (`invoiceId`) REFERENCES `invoice` (`id`)," +
        "CONSTRAINT `work_ibfk_2` FOREIGN KEY (`targetId`) REFERENCES `target` (`id`))";

        //PreparedStatement target = connection.prepareStatement("INSERT INTO warehouse.target (id, name, address, customerid) VALUES (?,?,?,?)");


        String target = "CREATE TABLE IF NOT EXISTS `target` (" +
        "`id` bigint(20) unsigned NOT NULL," +
        "`name` varchar(100) NOT NULL CHECK (`name` <> '')," +
        "`address` varchar(100) NOT NULL CHECK (`address` <> '')," +
        "`customerid` bigint(20) unsigned NOT NULL," +
        "PRIMARY KEY (`id`)," +
        "KEY `customerid` (`customerid`)," +
        "CONSTRAINT `target_ibfk_1` FOREIGN KEY (`customerid`) REFERENCES `customer` (`id`))";


        //PreparedStatement workTarget = connection.prepareStatement("INSERT INTO warehouse.worktarget (workId, targetId) VALUES (?,?)");

        String workTarget = "CREATE TABLE IF NOT EXISTS `worktarget` (" +
                "`workId` bigint(20) unsigned NOT NULL," +
                "`targetId` bigint(20) unsigned NOT NULL," +
                "PRIMARY KEY (`workId`,`targetId`)," +
                "KEY `workId` (`workId`)," +
                "KEY `targetId` (`targetId`)," +
                "CONSTRAINT `worktarget_ibfk_1` FOREIGN KEY (`workId`) REFERENCES `work` (`id`)," +
                "CONSTRAINT `worktarget_ibfk_2` FOREIGN KEY (`targetId`) REFERENCES `target` (`id`))";

        //PreparedStatement workHours = connection.prepareStatement("INSERT INTO warehouse.workhours (worktypeId, hours, discount, workId) VALUES(?,?,?,?)");

        String workHours = "CREATE TABLE IF NOT EXISTS `workhours` (" +
        "`worktypeId` bigint(20) unsigned NOT NULL," +
        "`hours` int(11) NOT NULL," +
        "`discount` decimal(65,2) DEFAULT NULL," +
        "`workId` bigint(20) unsigned NOT NULL," +
        "PRIMARY KEY (`workId`,`worktypeId`)," +
        "KEY `worktypeId` (`worktypeId`)," +
        "CONSTRAINT `workhours_ibfk_1` FOREIGN KEY (`workId`) REFERENCES `work` (`id`)," +
        "CONSTRAINT `workhours_ibfk_2` FOREIGN KEY (`worktypeId`) REFERENCES `work` (`id`))";

        String workType = "CREATE TABLE IF NOT EXISTS `worktype` (" +
        "`id` bigint(20) unsigned NOT NULL," +
        "`name` varchar(20) NOT NULL CHECK (`name` <> '')," +
        "`price` decimal(65,2) NOT NULL," +
        "PRIMARY KEY (`id`))";


        String warehouseItem = "CREATE TABLE IF NOT EXISTS `warehouseitem` (" +
        "`id` bigint(20) unsigned NOT NULL," +
        "`name` varchar(100) NOT NULL CHECK (`name` <> '')," +
        "`balance` int(11) NOT NULL," +
        "`unit` varchar(10) NOT NULL CHECK (`unit` <> '')," +
        "`purchaseprice` decimal(65,2) NOT NULL," +
        "`vat` decimal(65,2) NOT NULL," +
        "`removed` tinyint(1) NOT NULL DEFAULT 0," +
        "PRIMARY KEY (`id`))";


       /*
        System.out.println(createWarehouse + ";");
        System.out.println();
        System.out.println(customer + ";");
        System.out.println();
        System.out.println(useditem + ";");
        System.out.println();
        System.out.println(invoice + ";");
        System.out.println();
        System.out.println(work + ";");
        System.out.println();
        System.out.println(target + ";");
        System.out.println();
        System.out.println(workhours + ";");
        System.out.println();
        System.out.println(worktype + ";");
        System.out.println();
        System.out.println(warehouseitem + ";");
         */


        executeSQLUpdate("DROP DATABASE `" + database + "`");

        executeSQLUpdate(createWarehouse);
        executeSQLUpdate(customer, "jdbc:mariadb://127.0.0.1/" +  database);
        executeSQLUpdate(warehouseItem, "jdbc:mariadb://127.0.0.1/" +  database);
        executeSQLUpdate(workType, "jdbc:mariadb://127.0.0.1/" +  database);
        executeSQLUpdate(invoice, "jdbc:mariadb://127.0.0.1/" +  database);
        executeSQLUpdate(target, "jdbc:mariadb://127.0.0.1/" +  database);
        executeSQLUpdate(work, "jdbc:mariadb://127.0.0.1/" +  database);
        executeSQLUpdate(workInvoice, "jdbc:mariadb://127.0.0.1/" +  database);
        executeSQLUpdate(workTarget, "jdbc:mariadb://127.0.0.1/" +  database);
        executeSQLUpdate(usedItem, "jdbc:mariadb://127.0.0.1/" +  database);
        executeSQLUpdate(workHours, "jdbc:mariadb://127.0.0.1/" +  database);

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

            createBasicData(session);

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

                    Random r = new Random();

                    List<Integer> itemIndexes = new ArrayList<Integer>();

                    for(int j=0; j<itemFactor; j++) {

                        int itemIndex = r.nextInt(9);
                        while(itemIndexes.contains(itemIndex)) {
                            itemIndex = r.nextInt(9);
                        }

                        itemIndexes.add(itemIndex);

                    }

                    System.out.println("size: " + itemIndexes.size());

                    DataGeneratorThread thread = new DataGeneratorThread(i, iterationsPerThread, batchExecuteValue, customerFactor, invoiceFactor, targetFactor, workFactor, itemFactor, sequentialInvoices, firstnames, surnames, addresses, customerIndex, invoiceIndex, targetIndex, workIndex, itemIndexes);
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