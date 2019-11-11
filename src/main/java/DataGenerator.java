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
import java.util.concurrent.TimeUnit;

public class DataGenerator
{

    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DB_URL = "jdbc:mariadb://127.0.0.1/";

    //  Database credentials
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

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
                    +e.getMessage());
        }

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


    public void insertData(int iterationCount, int customerFactor, int invoiceFactor, int sequentialInvoices, int targetFactor, int workFactor, int itemFactor) {

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

            for(int i=0; i < iterationCount; i++) {
                insertRow(i, session);
            }

            session.close();
            driver.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    int customerIndex = 0;
    int invoiceIndex = 0;
    int targetIndex = 0;
    int workIndex = 0;

    int firstnameindex = 0;
    int surnameindex = 0;
    int addressindex = 0;


    public void resetIndexes() {

        if (firstnameindex >= firstnames.size()) {
            firstnameindex = 0;
        }

        if (surnameindex >= surnames.size()) {
            surnameindex = 0;
        }

        if (addressindex >= addresses.size()) {
            addressindex = 0;

        }

    }

    public void insertRow(int index, Session session) {

        Faker faker = new Faker();

        int i = 0;
        int j = 0;
        int k = 0;

        int customerIndexOriginal=customerIndex;

        while(i < customerFactor) {

            resetIndexes();

            String name = firstnames.get(firstnameindex) + " " + surnames.get(surnameindex);

            String streetAddress = addresses.get(addressindex).get("street") + " " + addresses.get(addressindex).get("city") + " " + addresses.get(addressindex).get("district") + " " + addresses.get(addressindex).get("region") + " " + addresses.get(addressindex).get("postcode");

            String sqlInsert = "INSERT INTO warehouse.customer (id, name, address) VALUES (" + customerIndex + ",\"" + name + "\",\"" + streetAddress + "\")";

            executeSQLInsert(sqlInsert);

            String cypherCreate = "CREATE (a:customer {customerId: " + customerIndex + ", name:\"" + name + "\",address:\"" + streetAddress + "\"})";
            session.run(cypherCreate);

            i++;
            customerIndex++;
            firstnameindex++;
            surnameindex++;
            addressindex++;

        }

        customerIndex=customerIndexOriginal;
        int invoiceIndexOriginal=invoiceIndex;

        String cypherCreate = null;

        i = 0;
        while (i < customerFactor) {
            int customerInvoiceIndexOriginal=invoiceIndex;
            j = 0;
            while (j < invoiceFactor) {

                Random r = new Random(index);
                //-- 0 = incomplete, 1 = complete, 2 = sent, 3 = paid
                int state = 1 + r.nextInt(3);

                GregorianCalendar gregorianCalendar = new GregorianCalendar();

                int year = Calendar.getInstance().get(Calendar.YEAR);


                gregorianCalendar.set(gregorianCalendar.YEAR, year);

                int dayOfYear = 1 + r.nextInt(gregorianCalendar.getActualMaximum(gregorianCalendar.DAY_OF_YEAR));

                gregorianCalendar.set(gregorianCalendar.DAY_OF_YEAR, dayOfYear);

                java.util.Date dueDate = gregorianCalendar.getTime();
                DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
                String dueDateAsString = dateFormat.format(dueDate);

                String sqlInsert = "";

                if(j < sequentialInvoices ) {

                    if(invoiceIndex == customerInvoiceIndexOriginal) {
                        sqlInsert = "INSERT INTO warehouse.invoice (id, customerId, state, duedate, previousinvoice) VALUES (" + invoiceIndex + "," + customerIndex + "," + state + ",STR_TO_DATE('" + dueDateAsString + "','%d-%m-%Y')," + invoiceIndex + ")";

                    }
                    else {
                        sqlInsert = "INSERT INTO warehouse.invoice (id, customerId, state, duedate, previousinvoice) VALUES (" + invoiceIndex + "," + customerIndex + "," + state + ",STR_TO_DATE('" + dueDateAsString + "','%d-%m-%Y')," + (invoiceIndex-1) + ")";

                    }

                } else {

                    sqlInsert = "INSERT INTO warehouse.invoice (id, customerId, state, duedate, previousinvoice) VALUES (" + invoiceIndex + "," + customerIndex + "," + state + ",STR_TO_DATE('" + dueDateAsString + "','%d-%m-%Y')," + invoiceIndex + ")";

                }
                executeSQLInsert(sqlInsert);


                LocalDate localDate = dueDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                int month = localDate.getMonthValue();
                int day = localDate.getDayOfMonth();

                if(j < sequentialInvoices ) {

                    if(invoiceIndex == customerInvoiceIndexOriginal) {

                        cypherCreate = "CREATE (l:invoice {invoiceId: " + invoiceIndex + ", customerId: " + customerIndex + ", state: " + state + ", duedate: \"date({ year:" + year + ", month:" + month + ", day:" + day + " })\",firstinvoice: " + customerInvoiceIndexOriginal + ", previousinvoice: " + invoiceIndex + "})";
                        session.run(cypherCreate);

                        cypherCreate = "MATCH (a:customer),(l:invoice) WHERE a.customerId = " + customerIndex + " AND l.invoiceId = " + invoiceIndex + " CREATE (a)-[m:PAYS]->(l)";
                        session.run(cypherCreate);

                    }
                    else {

                        cypherCreate = "CREATE (l:invoice {invoiceId: " + invoiceIndex + ", customerId: " + customerIndex + ", state: " + state + ", duedate: \"date({ year:" + year + ", month:" + month + ", day:" + day + " })\",firstinvoice: " + customerInvoiceIndexOriginal + ", previousinvoice: " + (invoiceIndex-1) + "})";
                        session.run(cypherCreate);

                        cypherCreate = "MATCH (a:customer),(l:invoice) WHERE a.customerId = " + customerIndex + " AND l.invoiceId = " + invoiceIndex + " CREATE (a)-[m:PAYS]->(l)";
                        session.run(cypherCreate);

                        cypherCreate = "MATCH (a:invoice),(b:invoice) WHERE a.invoiceId = " + (invoiceIndex-1) + " AND b.invoiceId = " + invoiceIndex + " CREATE (a)-[m:PREVIOUS_INVOICE]->(b)";
                        session.run(cypherCreate);


                    }

                } else {

                    cypherCreate = "CREATE (l:invoice {invoiceId: " + invoiceIndex + ", customerId: " + customerIndex + ", state: " + state + ", duedate: \"date({ year:" + year + ", month:" + month + ", day:" + day + " })\",firstinvoice: " + invoiceIndex + ", previousinvoice: " + invoiceIndex + "})";
                    session.run(cypherCreate);

                    cypherCreate = "MATCH (a:customer),(l:invoice) WHERE a.customerId = " + customerIndex + " AND l.invoiceId = " + invoiceIndex + " CREATE (a)-[m:PAYS]->(l)";
                    session.run(cypherCreate);

                }

                invoiceIndex++;
                j++;
            }

            customerIndex++;
            i++;
        }


        customerIndex=customerIndexOriginal;
        int targetIndexOriginal=targetIndex;

        i=0;
        while (i < customerFactor) {
            j=0;
            while(j < targetFactor) {

                resetIndexes();

                String name = firstnames.get(firstnameindex) + " " + surnames.get(surnameindex);

                String streetAddress = addresses.get(addressindex).get("street") + " " + addresses.get(addressindex).get("city") + " " + addresses.get(addressindex).get("district") + " " + addresses.get(addressindex).get("region") + " " + addresses.get(addressindex).get("postcode");

                String sqlInsert = "INSERT INTO warehouse.target (id, name, address, customerid) VALUES (" + targetIndex +  ",\"" + name + "\",\"" + streetAddress + "\"," + customerIndex + ")";
                executeSQLInsert(sqlInsert);

                cypherCreate = "CREATE (t:target {tyotargetId: " + targetIndex + ", name: \"" + name + "\", address: \"" + streetAddress + "\", customerid: " + customerIndex + " })";
                session.run(cypherCreate);

                cypherCreate = "MATCH (a:customer),(t:target) WHERE a.customerId = " + customerIndex + " AND t.tyotargetId = " + targetIndex + " CREATE (a)-[m:CUSTOMER_TARGET]->(t)";
                session.run(cypherCreate);

                targetIndex++;
                firstnameindex++;
                surnameindex++;
                addressindex++;
                j++;
            }


            customerIndex++;
            i++;
        }


        int workIndexOriginal=workIndex;
        targetIndex=targetIndexOriginal;
        invoiceIndex=invoiceIndexOriginal;

        j=0;
        while(j < workFactor) {

            Random r = new Random(index);

            int price = 1 + r.nextInt(1001);
            //-- 0 = incomplete, 1 = complete, 2 = sent, 3 = paid

            String name = "Generic";

            String sqlInsert = "INSERT INTO warehouse.work (id, name) VALUES (" + workIndex + ",'" + name + "')";
            executeSQLInsert(sqlInsert);

            cypherCreate = "CREATE (s:work {workId: " + workIndex + ", name: \"" + name + "\"})";
            session.run(cypherCreate);

            cypherCreate = "MATCH (s:work),(l:invoice) WHERE s.workId = " + workIndex + " AND l.invoiceId = " + invoiceIndex + " CREATE (s)-[m:WORK_INVOICE]->(l)";
            session.run(cypherCreate);

            cypherCreate = "MATCH (s:work),(t:target) WHERE s.workId = " + workIndex + "  AND t.tyotargetId = " + targetIndex + " CREATE (s)-[m:WORK_INVOICE]->(t)";
            session.run(cypherCreate);

            workIndex++;
            j++;
        }

        workIndex=workIndexOriginal;

        i=0;
        while(i < workFactor) {

            targetIndex=targetIndexOriginal;

            j = 0;
            while(j < targetFactor*customerFactor) {

                String sqlInsert = "INSERT INTO warehouse.worktarget (workId, targetId) VALUES (" + workIndex + "," + targetIndex + ")";
                executeSQLInsert(sqlInsert);

                cypherCreate = "MATCH (s:work),(t:target) WHERE s.workId = " + workIndex + "  AND t.tyotargetId = " + targetIndex + " CREATE (s)-[m:WORK_TARGET]->(t)";
                session.run(cypherCreate);

                targetIndex++;
                j++;

            }

            workIndex++;
            i++;

        }

        workIndex=workIndexOriginal;


        i=0;
        while(i < workFactor) {

            invoiceIndex=invoiceIndexOriginal;

            j=0;
            while(j < invoiceFactor*customerFactor) {

                Random r = new Random(index);

                int price = 1 + r.nextInt(1001);
                //-- 0 = incomplete, 1 = complete, 2 = sent, 3 = paid

                r = new Random(index);

                int type = 1 + r.nextInt(4); // tämä on turha

                String sqlInsert = "INSERT INTO warehouse.workinvoice (workId, invoiceId) VALUES (" + workIndex + "," + invoiceIndex + ")";
                executeSQLInsert(sqlInsert);

                cypherCreate = "MATCH (s:work),(l:invoice) WHERE s.workId = " + workIndex + " AND l.invoiceId = " + invoiceIndex + " CREATE (s)-[m:WORK_INVOICE]->(l)";
                session.run(cypherCreate);


                invoiceIndex++;
                j++;

            }

            workIndex++;
            i++;
        }

        workIndex=workIndexOriginal;

        Random r = new Random(index);

        int discountpercent = 1 + r.nextInt(101);
        double discount = (0.01 * discountpercent);

        i=0;
        while(i < workFactor) {

            j = 0;
            while (j <= itemFactor) {

                r = new Random(index);

                int amount = 1 + r.nextInt(101);

                int warehouseitemId = j;

                String sqlInsert = "INSERT INTO warehouse.useditem (amount, discount, workId, warehouseitemId) VALUES(" + amount + "," + discount + "," + workIndex + "," + warehouseitemId + ")";
                executeSQLInsert(sqlInsert);

                cypherCreate = "MATCH (s:work),(v:warehouseitem) WHERE s.workId=" + workIndex + " AND v.warehouseitemId=" + warehouseitemId +
                    " CREATE (s)-[i1:USED_ITEM {amount:" + amount + ", discount:" + discount + "}]->(v)" +
                    " CREATE (v)-[i2:USED_ITEM {amount:" + amount + ", discount:" + discount + "}]->(s)";

                System.out.println(cypherCreate);
                session.run(cypherCreate);

                j++;

            }

            i++;
            workIndex++;
        }


        workIndex=workIndexOriginal;

        i=0;
        while(i < workFactor) {

            j = 0;
            while (j < 3) {

                r = new Random(index);

                int hours = r.nextInt(100);
                int worktypeId = j;

                String sqlInsert = "INSERT INTO warehouse.workhours (worktypeId, hours, discount, workId) VALUES(" + worktypeId + "," + hours + "," + discount + "," + workIndex + ")";
                executeSQLInsert(sqlInsert);

                cypherCreate = "MATCH (w:work),(wt:worktype) WHERE w.workId=" + workIndex + " AND wt.worktypeId=" + worktypeId +
                        " CREATE (wt)-[h1:WORKHOURS {hours:" + hours + ", discount:" + discount + "}]->(w)" +
                        " CREATE (w)-[h2:WORKHOURS {hours:" + hours + ", discount:" + discount + "}]->(wt)";
                System.out.println(cypherCreate);
                session.run(cypherCreate);
                j++;

            }

            i++;
            workIndex++;
        }

    }

}