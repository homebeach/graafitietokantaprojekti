import com.github.javafaker.Faker;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DataGenerator {

    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DB_URL = "jdbc:mariadb://127.0.0.1/";

    //  Database credentials
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    private int iterationsPerThread = 0;
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

            org.neo4j.driver.v1.Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "admin"));

            Session session = driver.session();

            session.run("MATCH (n) DETACH DELETE n");

            session.close();
            driver.close();

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

            ResultSet rs = executeSQLQuery("SELECT firstname FROM testdata.firstnames;");
            while (rs.next()) {
                String firstName = rs.getString("firstname");
                firstnames.add(firstName);
            }

            rs = executeSQLQuery("SELECT surname FROM testdata.surnames;");
            while (rs.next()) {
                String surName = rs.getString("surname");
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

    public void createItems(int itemCount) {


        try {

            org.neo4j.driver.v1.Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "admin"));

            Session session = driver.session();

            Connection connection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);

            PreparedStatement warehouseitem = connection.prepareStatement("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (?,?,?,?,?,?,?)");

            for (int i = 0; i < itemCount; i++) {

                Random r = new Random(i);
                int balance = r.nextInt(100);
                r.setSeed(i);
                float purchaseprice = r.nextFloat();
                r.setSeed(i);
                int vat = r.nextInt(50);
                r.setSeed(i);
                boolean removed = r.nextBoolean();

                if (i % 2 == 0) {

                    r.setSeed(i);
                    r = new Random(i);
                    int x = r.nextInt(10);
                    r.setSeed(i + 1);
                    int y = r.nextInt(10);
                    r.setSeed(i + 2);
                    int size = r.nextInt(10);

                    String item = "MMJ " + x + "X" + y + "," + size + "MM²CABLE";

                    //executeSQLUpdate("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (" + i + ", '" + item + "'," + balance + ", 'm'," + purchaseprice + "," + vat + "," + removed + ")");

                    warehouseitem.setInt(1, i);
                    warehouseitem.setString(2, item);
                    warehouseitem.setInt(3, balance);
                    warehouseitem.setString(4, "'m'");
                    warehouseitem.setFloat(5, purchaseprice);
                    warehouseitem.setInt(6, vat);
                    warehouseitem.setBoolean(7, removed);

                    session.run("CREATE (v:warehouseitem {warehouseitemId: " + i + ", name: \"" + item + "\", balance:" + balance + ", unit:\"m\", purchaseprice:" + purchaseprice + ", vat:" + vat + ", removed:" + removed + "})");


                } else if (i % 3 == 0) {

                    r.setSeed(i);
                    int ground = r.nextInt(10);
                    String item = "SOCKET " + ground + "-GROUND OL JUSSI";

                    //executeSQLUpdate("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (" + i + ", '" + item + "'," + balance + ", 'pcs'," + purchaseprice + "," + vat + "," + removed + ")");

                    warehouseitem.setInt(1, i);
                    warehouseitem.setString(2, item);
                    warehouseitem.setInt(3, balance);
                    warehouseitem.setString(4, "'pcs'");
                    warehouseitem.setFloat(5, purchaseprice);
                    warehouseitem.setInt(6, vat);
                    warehouseitem.setBoolean(7, removed);

                    session.run("CREATE (v:warehouseitem {warehouseitemId: " + i + ", name:\"" + item + "\", balance:" + balance + ", unit:\"pcs\", purchaseprice:" + purchaseprice + ", vat:" + vat + ", removed:" + removed + "})");

                } else if (i % 5 == 0) {

                    r.setSeed(i);
                    int spiral1 = r.nextInt(10);
                    r.setSeed(i + 1);
                    int spiral2 = r.nextInt(10);
                    r.setSeed(i + 2);
                    int spiral3 = r.nextInt(100);

                    String item = "BINDING SPIRAL " + spiral1 + "," + spiral2 + "-" + spiral3 + "MM INVISIBLE";

                    //executeSQLUpdate("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (" + i + ", '" + item + "'," + balance + ", 'pcs'," + purchaseprice + "," + vat + "," + removed + ")");

                    warehouseitem.setInt(1, i);
                    warehouseitem.setString(2, item);
                    warehouseitem.setInt(3, balance);
                    warehouseitem.setString(4, "'pcs'");
                    warehouseitem.setFloat(5, purchaseprice);
                    warehouseitem.setInt(6, vat);
                    warehouseitem.setBoolean(7, removed);

                    session.run("CREATE (v:warehouseitem {warehouseitemId: " + i + ", name:\"" + item + "\", balance:" + balance + ", unit:\"pcs\", purchaseprice:" + purchaseprice + ", vat:" + vat + ", removed:" + removed + "})");


                } else {

                    r.setSeed(i);
                    int parts = r.nextInt(10);

                    String item = "SOCKET CORNER MODEL " + parts + "-PARTS";

                    //executeSQLUpdate("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (" + i + ", '" + item + "'," + balance + ", 'pcs'," + purchaseprice + "," + vat + "," + removed + ")");

                    warehouseitem.setInt(1, i);
                    warehouseitem.setString(2, item);
                    warehouseitem.setInt(3, balance);
                    warehouseitem.setString(4, "'pcs'");
                    warehouseitem.setFloat(5, purchaseprice);
                    warehouseitem.setInt(6, vat);
                    warehouseitem.setBoolean(7, removed);

                    session.run("CREATE (v:warehouseitem {warehouseitemId: " + i + ", name:\"" + item + "\", balance:" + balance + ", unit:\"pcs\", purchaseprice:" + purchaseprice + ", vat:" + vat + ", removed:" + removed + "})");

                }

                warehouseitem.execute();

            }

            session.close();
            driver.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void createWorkTypes(int workTypeCount) {


        try {

            org.neo4j.driver.v1.Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "admin"));

            Session session = driver.session();

            Connection connection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);

            PreparedStatement worktype = connection.prepareStatement("INSERT INTO warehouse.worktype (id, name, price) VALUES (?, ?, ?)");

            for (int i = 0; i < workTypeCount; i++) {


                Random r = new Random(i);
                int price = r.nextInt(100);

                worktype.setInt(1, i);
                worktype.setInt(3, price);

                if(i % 2 == 0) {

                    worktype.setString(2,"design");
                    session.run("CREATE (wt:worktype {worktypeId: " + i + ", name:\"design\", price:" + price + "})");

                } else if(i % 3 == 0) {

                    worktype.setString(2,"work");
                    session.run("CREATE (wt:worktype {worktypeId: " + i + ", name:\"work\", price:" + price + "})");

                } else {

                    worktype.setString(2,"supporting work");
                    session.run("CREATE (wt:worktype {worktypeId: " + i + ", name:\"supporting work\", price:" + price + "})");

                }

                worktype.execute();

            }

            session.close();
            driver.close();


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void createSampleTables() {

        String database = "testdata";

        String dropDatabase = "DROP DATABASE `" + database + "`";

        String createDatabase = "CREATE DATABASE IF NOT EXISTS `" + database + "`";

        String firstnames = "CREATE TABLE IF NOT EXISTS `firstnames` (" +
                "`id` serial," +
                "`firstname` varchar(100) NOT NULL," +
                "PRIMARY KEY (`id`))";

        String surnames = "CREATE TABLE IF NOT EXISTS `surnames` (" +
                "`id` serial," +
                "`surname` varchar(100) NOT NULL," +
                "PRIMARY KEY (`id`))";

        String addresses = "CREATE TABLE IF NOT EXISTS `addresses` (" +
                "`id` serial," +
                "`street` varchar(200) NOT NULL," +
                "`city` varchar(100) NOT NULL," +
                "`district` varchar(100) NOT NULL," +
                "`region` varchar(50) NOT NULL," +
                "`postcode` varchar(50) NOT NULL," +
                "PRIMARY KEY (`id`))";

        executeSQLUpdate(dropDatabase);
        executeSQLUpdate(createDatabase);
        executeSQLUpdate(firstnames, "jdbc:mariadb://127.0.0.1/" +  database);
        executeSQLUpdate(surnames, "jdbc:mariadb://127.0.0.1/" +  database);
        executeSQLUpdate(addresses, "jdbc:mariadb://127.0.0.1/" +  database);

    }


    public void loadSampleData(int batchExecuteValue) {

            String firstnamesFile = "./data/firstnames.csv";
            String surnamesFile = "./data/surnames.csv";
            String addressesFile = "./data/city_of_houston.csv";

            BufferedReader br = null;
            String line = "";
            String cvsSplitBy = ",";


            try {

                Connection connection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);

                PreparedStatement firstnames = connection.prepareStatement("INSERT INTO testdata.firstnames (firstname) VALUES (?)");
                PreparedStatement surnames = connection.prepareStatement("INSERT INTO testdata.surnames (surname) VALUES (?)");
                PreparedStatement addresses = connection.prepareStatement("INSERT INTO testdata.addresses (street,city,district,region,postcode) VALUES (?,?,?,?,?)");


                boolean firstIteration = true;

                int index = 0;

                br = new BufferedReader(new FileReader(firstnamesFile));
                while ((line = br.readLine()) != null) {

                    if(!firstIteration) {

                        // use comma as separator
                        String[] firstnameInArray = line.split(cvsSplitBy);

                        String firstname = firstnameInArray[0];

                        firstnames.setString(1, firstname);
                        firstnames.addBatch();

                        if (index % batchExecuteValue == 0) {

                            firstnames.executeBatch();

                        }

                    } else {

                        firstIteration = false;

                    }

                    index++;
                }

                firstnames.executeBatch();

                index = 0;

                firstIteration = true;

                br = new BufferedReader(new FileReader(surnamesFile));
                while ((line = br.readLine()) != null) {

                    if(!firstIteration) {

                        // use comma as separator
                        String[] surnameInArray = line.split(cvsSplitBy);

                        String surname = surnameInArray[0].toLowerCase();
                        surname = surname.substring(0, 1).toUpperCase() + surname.substring(1);

                        surnames.setString(1, surname);
                        surnames.addBatch();

                        if (index % batchExecuteValue == 0) {

                            surnames.executeBatch();

                        }

                    } else {

                        firstIteration = false;

                    }
                    index++;
                }

                surnames.executeBatch();

                index = 0;

                firstIteration = true;

                br = new BufferedReader(new FileReader(addressesFile));
                while ((line = br.readLine()) != null) {

                    if(!firstIteration) {

                        String[] addressInArray = line.split(cvsSplitBy);

                        String street = addressInArray[3].toLowerCase();

                        if(street.length() > 1) {
                            street = street.substring(0, 1).toUpperCase() + street.substring(1);
                        }

                        String city = addressInArray[5].toLowerCase();

                        if(city.length() > 1) {
                            city = city.substring(0, 1).toUpperCase() + city.substring(1);
                        }

                        String district = addressInArray[6].toLowerCase();

                        if(district.length() > 1) {
                            district = district.substring(0, 1).toUpperCase() + district.substring(1);
                        }


                        String region = addressInArray[7].toLowerCase();

                        if(region.length() > 1) {
                            region = region.substring(0, 1).toUpperCase() + region.substring(1);
                        }

                        String postcode = addressInArray[8];

                        addresses.setString(1, street);
                        addresses.setString(2, city);
                        addresses.setString(3, district);
                        addresses.setString(4, region);
                        addresses.setString(5, postcode);
                        addresses.addBatch();

                        if (index % batchExecuteValue == 0) {

                            addresses.executeBatch();

                        }

                    } else {

                        firstIteration = false;

                    }
                    index++;
                }

                addresses.executeBatch();


            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                if (br != null) {
                    try {
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
    }



    public void createTables() {


        String database = "warehouse";

        String dropDatabase = "DROP DATABASE `" + database + "`";

        String createDatabase = "CREATE DATABASE IF NOT EXISTS `" + database + "`";

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
        "`name` varchar(100) NOT NULL CHECK (`name` <> '')," +
        "PRIMARY KEY (`id`))";

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
        "KEY `workId` (`workId`)," +
        "CONSTRAINT `workhours_ibfk_1` FOREIGN KEY (`workId`) REFERENCES `work` (`id`)," +
        "CONSTRAINT `workhours_ibfk_2` FOREIGN KEY (`worktypeId`) REFERENCES `worktype` (`id`))";

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


        executeSQLUpdate(dropDatabase);
        executeSQLUpdate(createDatabase);
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



    public void insertData(int threadCount, int iterationsPerThread, int batchExecuteValue, int invoiceFactor, int sequentialInvoices, int targetFactor, int workFactor, int workTypeFactor, int itemFactor) {

        this.iterationsPerThread = iterationsPerThread;
        this.invoiceFactor = invoiceFactor;
        this.sequentialInvoices = sequentialInvoices;
        this.targetFactor = targetFactor;
        this.workFactor = workFactor;
        this.itemFactor = itemFactor;

        try {

            org.neo4j.driver.v1.Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "admin"));

            Session session = driver.session();



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

                ResultSet rs = executeSQLQuery("SELECT COUNT(*) AS WAREHOUSEITEMCOUNT FROM WAREHOUSE.WAREHOUSEITEM");

                int itemCount = 0;

                while(rs.next()) {

                    itemCount = rs.getInt("WAREHOUSEITEMCOUNT");
                    System.out.println("This is itemcount " + itemCount);

                }


                rs = executeSQLQuery("SELECT COUNT(*) AS WORKTYPECOUNT FROM WAREHOUSE.WORKTYPE");

                int workTypeCount = 0;

                while(rs.next()) {

                    workTypeCount = rs.getInt("WORKTYPECOUNT");
                    System.out.println("This is worktypecount " + workTypeCount);

                }

                ExecutorService executor = Executors.newFixedThreadPool(threadCount);

                long startTimeInMilliseconds = System.currentTimeMillis();

                Timestamp startTime = new Timestamp(startTimeInMilliseconds);

                System.out.println("Insertion started at: " + startTime.toString());

                for (int i = 0; i < threadCount; i++) {

                    Random r = new Random();

                    List<Integer> itemIndexes = new ArrayList<Integer>();
                    for(int j=0; j<itemFactor; j++) {

                        r.setSeed(j);

                        int itemIndex = r.nextInt(itemCount);

                        int offset = 1;
                        while(itemIndexes.contains(itemIndex)) {
                            r.setSeed(j + offset);
                            itemIndex = r.nextInt(itemCount);
                            offset++;
                        }

                        itemIndexes.add(itemIndex);

                    }

                    List<Integer> workTypeIndexes = new ArrayList<Integer>();
                    for(int j=0; j<workTypeFactor; j++) {

                        r.setSeed(j);

                        int workTypeIndex = r.nextInt(workTypeCount);

                        int offset = 1;
                        while(workTypeIndexes.contains(workTypeIndex)) {

                            r.setSeed(j + offset);
                            workTypeIndex = r.nextInt(workTypeCount);
                            offset++;
                        }

                        workTypeIndexes.add(workTypeIndex);

                    }



                    DataGeneratorThread thread = new DataGeneratorThread(i, iterationsPerThread, batchExecuteValue, invoiceFactor, targetFactor, workFactor, itemFactor, sequentialInvoices, firstnames, surnames, addresses, customerIndex, invoiceIndex, targetIndex, workIndex, itemIndexes, workTypeIndexes);

                    executor.execute(thread);
                    customerIndex = customerIndex + iterationsPerThread;
                    invoiceIndex = invoiceIndex + iterationsPerThread*invoiceFactor;
                    targetIndex = targetIndex + iterationsPerThread*targetFactor;
                    workIndex = workIndex + iterationsPerThread*workFactor;

                }

                executor.shutdown();
                while (!executor.isTerminated()) {
                }

                long endTimeInMilliseconds = System.currentTimeMillis();

                Timestamp endTime = new Timestamp(endTimeInMilliseconds);


                long elapsedTimeMilliseconds = endTimeInMilliseconds - startTimeInMilliseconds;

                String elapsedTime = (new SimpleDateFormat("mm:ss:SSS")).format(new Date(elapsedTimeMilliseconds));

                Instant end = Instant.now();

                System.out.println("Insertion finished at: " + endTime.toString());
                System.out.println("Time elapsed: " + elapsedTime);

            }

            session.close();
            driver.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}