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

public class DataGeneratorThread extends Thread {

    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DB_URL = "jdbc:mariadb://127.0.0.1/";

    //  Database credentials
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    private int threadindex = 0;
    private int iterationCount = 0;
    private int batchExecuteValue = 0;
    private int customerFactor = 0;
    private int invoiceFactor = 0;
    private int targetFactor = 0;
    private int workFactor = 0;
    private int itemFactor = 0;
    private int sequentialInvoices = 0;

    private List<Integer> itemIndexes;


    private List<String> firstnames;
    private List<String> surnames;
    private List<HashMap<String, String>> addresses;

    public DataGeneratorThread(int threadindex, int iterationCount, int batchExecuteValue, int customerFactor, int invoiceFactor, int targetFactor, int workFactor, int itemFactor, int sequentialInvoices, List<String> firstnames, List<String> surnames, List<HashMap<String, String>> addresses, int customerIndex, int invoiceIndex, int targetIndex, int workIndex, List<Integer> itemIndexes) {

        this.threadindex = threadindex;
        this.iterationCount = iterationCount;
        this.batchExecuteValue = batchExecuteValue;
        this.customerFactor = customerFactor;
        this.invoiceFactor = invoiceFactor;
        this.targetFactor = targetFactor;
        this.workFactor = workFactor;
        this.itemFactor = itemFactor;
        this.sequentialInvoices = sequentialInvoices;
        this.firstnames = firstnames;
        this.surnames = surnames;
        this.addresses = addresses;
        this.customerIndex = customerIndex;
        this.invoiceIndex = invoiceIndex;
        this.targetIndex = targetIndex;
        this.workIndex = workIndex;
        this.itemIndexes = itemIndexes;
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

    public void run() {

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

                PreparedStatement warehouseItem = connection.prepareStatement("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (?,?,?,?,?,?,?)");

                PreparedStatement workTarget = connection.prepareStatement("INSERT INTO warehouse.worktarget (workId, targetId) VALUES (?,?)");

                PreparedStatement workInvoice = connection.prepareStatement("INSERT INTO warehouse.workinvoice (workId, invoiceId) VALUES (?,?)");
                PreparedStatement usedItem = connection.prepareStatement("INSERT INTO warehouse.useditem (amount, discount, workId, warehouseitemId) VALUES(?,?,?,?)");
                PreparedStatement workHours = connection.prepareStatement("INSERT INTO warehouse.workhours (worktypeId, hours, discount, workId) VALUES(?,?,?,?)");

                HashMap<String, PreparedStatement> preparedStatements = new HashMap<String, PreparedStatement>();

                preparedStatements.put("customer",customer);
                preparedStatements.put("invoice",invoice);
                preparedStatements.put("target",target);
                preparedStatements.put("warehouseItem",warehouseItem);
                preparedStatements.put("work",work);
                preparedStatements.put("worktarget",workTarget);
                preparedStatements.put("workinvoice",workInvoice);
                preparedStatements.put("useditem",usedItem);
                preparedStatements.put("workhours",workHours);

                for (int i = 0; i < iterationCount; i++) {

                    insertRow(i, batchExecuteValue, session, preparedStatements, itemIndexes);

                }


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

    public void insertRow(int index, int batchExecuteValue, Session session, HashMap<String, PreparedStatement> preparedStatements, List<Integer> itemIndexes) throws SQLException {

        PreparedStatement customer = preparedStatements.get("customer");
        PreparedStatement invoice = preparedStatements.get("invoice");
        PreparedStatement target = preparedStatements.get("target");
        PreparedStatement warehouseItem = preparedStatements.get("warehouseItem");
        PreparedStatement work = preparedStatements.get("work");
        PreparedStatement workTarget = preparedStatements.get("worktarget");
        PreparedStatement workInvoice = preparedStatements.get("workinvoice");
        PreparedStatement usedItem = preparedStatements.get("useditem");
        PreparedStatement workHours = preparedStatements.get("workhours");

        Faker faker = new Faker();

        int i = 0;
        int j = 0;
        int k = 0;

        int customerIndexOriginal = customerIndex;

        System.out.println("Thread: " + threadindex + " Index: " + index);

            while (i < customerFactor) {

                resetIndexes();

                String name = firstnames.get(firstnameindex) + " " + surnames.get(surnameindex);

                String streetAddress = addresses.get(addressindex).get("street") + " " + addresses.get(addressindex).get("city") + " " + addresses.get(addressindex).get("district") + " " + addresses.get(addressindex).get("region") + " " + addresses.get(addressindex).get("postcode");

                String sqlInsert = "INSERT INTO warehouse.customer (id, name, address) VALUES (" + customerIndex + ",\"" + name + "\",\"" + streetAddress + "\")";



                customer.setInt(1, customerIndex);
                customer.setString(2, name);
                customer.setString(3, streetAddress);
                customer.addBatch();

                String cypherCreate = "CREATE (a:customer {customerId: " + customerIndex + ", name:\"" + name + "\",address:\"" + streetAddress + "\"})";
                session.run(cypherCreate);

                i++;
                customerIndex++;
                firstnameindex++;
                surnameindex++;
                addressindex++;

            }

            customerIndex = customerIndexOriginal;
            int invoiceIndexOriginal = invoiceIndex;

            String cypherCreate = null;

            i = 0;
            while (i < customerFactor) {
                int customerInvoiceIndexOriginal = invoiceIndex;
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
                    java.sql.Date sqlDueDate = new java.sql.Date(gregorianCalendar.getTime().getTime());
                    DateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy");
                    String dueDateAsString = dateFormat.format(dueDate);

                    String sqlInsert = "";

                    if (j < sequentialInvoices) {

                        invoice.setInt(1, invoiceIndex);
                        invoice.setInt(2, customerIndex);
                        invoice.setInt(3, state);
                        invoice.setDate(4, sqlDueDate, gregorianCalendar);


                        if (invoiceIndex == customerInvoiceIndexOriginal) {
                            sqlInsert = "INSERT INTO warehouse.invoice (id, customerId, state, duedate, previousinvoice) VALUES (" + invoiceIndex + "," + customerIndex + "," + state + ",STR_TO_DATE('" + dueDateAsString + "','%d-%m-%Y')," + invoiceIndex + ")";
                            invoice.setInt(5, invoiceIndex);

                        } else {
                            sqlInsert = "INSERT INTO warehouse.invoice (id, customerId, state, duedate, previousinvoice) VALUES (" + invoiceIndex + "," + customerIndex + "," + state + ",STR_TO_DATE('" + dueDateAsString + "','%d-%m-%Y')," + (invoiceIndex - 1) + ")";
                            invoice.setInt(5,invoiceIndex - 1);
                        }

                        invoice.addBatch();

                    } else {

                        sqlInsert = "INSERT INTO warehouse.invoice (id, customerId, state, duedate, previousinvoice) VALUES (" + invoiceIndex + "," + customerIndex + "," + state + ",STR_TO_DATE('" + dueDateAsString + "','%d-%m-%Y')," + invoiceIndex + ")";

                        invoice.setInt(1, invoiceIndex);
                        invoice.setInt(2, customerIndex);
                        invoice.setInt(3, state);
                        invoice.setDate(4, sqlDueDate, gregorianCalendar);
                        invoice.setInt(5, invoiceIndex);
                        invoice.addBatch();

                    }

                    LocalDate localDate = dueDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
                    int month = localDate.getMonthValue();
                    int day = localDate.getDayOfMonth();

                    if (j < sequentialInvoices) {

                        if (invoiceIndex == customerInvoiceIndexOriginal) {

                            cypherCreate = "CREATE (l:invoice {invoiceId: " + invoiceIndex + ", customerId: " + customerIndex + ", state: " + state + ", duedate: \"date({ year:" + year + ", month:" + month + ", day:" + day + " })\",firstinvoice: " + customerInvoiceIndexOriginal + ", previousinvoice: " + invoiceIndex + "})";
                            session.run(cypherCreate);

                            cypherCreate = "MATCH (a:customer),(l:invoice) WHERE a.customerId = " + customerIndex + " AND l.invoiceId = " + invoiceIndex + " CREATE (a)-[m:PAYS]->(l)";
                            session.run(cypherCreate);

                        } else {

                            cypherCreate = "CREATE (l:invoice {invoiceId: " + invoiceIndex + ", customerId: " + customerIndex + ", state: " + state + ", duedate: \"date({ year:" + year + ", month:" + month + ", day:" + day + " })\",firstinvoice: " + customerInvoiceIndexOriginal + ", previousinvoice: " + (invoiceIndex - 1) + "})";
                            session.run(cypherCreate);

                            cypherCreate = "MATCH (a:customer),(l:invoice) WHERE a.customerId = " + customerIndex + " AND l.invoiceId = " + invoiceIndex + " CREATE (a)-[m:PAYS]->(l)";
                            session.run(cypherCreate);

                            cypherCreate = "MATCH (a:invoice),(b:invoice) WHERE a.invoiceId = " + (invoiceIndex - 1) + " AND b.invoiceId = " + invoiceIndex + " CREATE (a)-[m:PREVIOUS_INVOICE]->(b)";
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


            customerIndex = customerIndexOriginal;
            int targetIndexOriginal = targetIndex;

            i = 0;
            while (i < customerFactor) {
                j = 0;
                while (j < targetFactor) {

                    resetIndexes();

                    String name = firstnames.get(firstnameindex) + " " + surnames.get(surnameindex);

                    String streetAddress = addresses.get(addressindex).get("street") + " " + addresses.get(addressindex).get("city") + " " + addresses.get(addressindex).get("district") + " " + addresses.get(addressindex).get("region") + " " + addresses.get(addressindex).get("postcode");

                    String sqlInsert = "INSERT INTO warehouse.target (id, name, address, customerid) VALUES (" + targetIndex + ",\"" + name + "\",\"" + streetAddress + "\"," + customerIndex + ")";


                    target.setInt(1, targetIndex);
                    target.setString(2, name);
                    target.setString(3, streetAddress);
                    target.setInt(4, customerIndex);
                    target.addBatch();

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


            int workIndexOriginal = workIndex;
            targetIndex = targetIndexOriginal;
            invoiceIndex = invoiceIndexOriginal;

            j = 0;
            while (j < workFactor) {

                Random r = new Random(index);

                int price = 1 + r.nextInt(1001);
                //-- 0 = incomplete, 1 = complete, 2 = sent, 3 = paid

                String name = "Generic " + workIndex;

                String sqlInsert = "INSERT INTO warehouse.work (id, name) VALUES (" + workIndex + ",'" + name + "')";


                work.setInt(1, workIndex);
                work.setString(2, name);
                work.addBatch();

                cypherCreate = "CREATE (s:work {workId: " + workIndex + ", name: \"" + name + "\"})";
                session.run(cypherCreate);

                cypherCreate = "MATCH (s:work),(l:invoice) WHERE s.workId = " + workIndex + " AND l.invoiceId = " + invoiceIndex + " CREATE (s)-[m:WORK_INVOICE]->(l)";
                session.run(cypherCreate);

                cypherCreate = "MATCH (s:work),(t:target) WHERE s.workId = " + workIndex + "  AND t.tyotargetId = " + targetIndex + " CREATE (s)-[m:WORK_INVOICE]->(t)";
                session.run(cypherCreate);

                workIndex++;
                j++;
            }

            workIndex = workIndexOriginal;

            i = 0;
            while (i < workFactor) {

                targetIndex = targetIndexOriginal;

                j = 0;
                while (j < targetFactor * customerFactor) {

                    String sqlInsert = "INSERT INTO warehouse.worktarget (workId, targetId) VALUES (" + workIndex + "," + targetIndex + ")";
                    //executeSQLInsert(sqlInsert);

                    workTarget.setInt(1, workIndex);
                    workTarget.setInt(2, targetIndex);
                    workTarget.addBatch();

                    cypherCreate = "MATCH (s:work),(t:target) WHERE s.workId = " + workIndex + "  AND t.tyotargetId = " + targetIndex + " CREATE (s)-[m:WORK_TARGET]->(t)";
                    session.run(cypherCreate);

                    targetIndex++;
                    j++;

                }

                workIndex++;
                i++;

            }

            workIndex = workIndexOriginal;


            i = 0;
            while (i < workFactor) {

                invoiceIndex = invoiceIndexOriginal;

                j = 0;
                while (j < invoiceFactor * customerFactor) {

                    Random r = new Random(index);

                    int price = 1 + r.nextInt(1001);
                    //-- 0 = incomplete, 1 = complete, 2 = sent, 3 = paid

                    r = new Random(index);

                    int type = 1 + r.nextInt(4); // tämä on turha

                    String sqlInsert = "INSERT INTO warehouse.workinvoice (workId, invoiceId) VALUES (" + workIndex + "," + invoiceIndex + ")";

                    workInvoice.setInt(1, workIndex);
                    workInvoice.setInt(2, invoiceIndex);
                    workInvoice.addBatch();

                    cypherCreate = "MATCH (s:work),(l:invoice) WHERE s.workId = " + workIndex + " AND l.invoiceId = " + invoiceIndex + " CREATE (s)-[m:WORK_INVOICE]->(l)";
                    session.run(cypherCreate);


                    invoiceIndex++;
                    j++;

                }

                workIndex++;
                i++;
            }

            workIndex = workIndexOriginal;







            Random r = new Random(index);
            int discountpercent = 1 + r.nextInt(101);
            double discount = (0.01 * discountpercent);

            i = 0;
            while (i < workFactor) {

                j = 0;
                while (j < itemIndexes.size()) {

                    r = new Random(index);

                    int amount = 1 + r.nextInt(101);

                    int warehouseitemId = itemIndexes.get(j);

                    String sqlInsert = "INSERT INTO warehouse.useditem (amount, discount, workId, warehouseitemId) VALUES(" + amount + "," + discount + "," + workIndex + "," + warehouseitemId + ")";

                    System.out.println(sqlInsert);

                    usedItem.setInt(1, amount);
                    usedItem.setDouble(2, discount);
                    usedItem.setInt(3, workIndex);
                    usedItem.setInt(4, warehouseitemId);
                    usedItem.addBatch();

                    cypherCreate = "MATCH (s:work),(v:warehouseitem) WHERE s.workId=" + workIndex + " AND v.warehouseitemId=" + warehouseitemId +
                            " CREATE (s)-[i1:USED_ITEM {amount:" + amount + ", discount:" + discount + "}]->(v)" +
                            " CREATE (v)-[i2:USED_ITEM {amount:" + amount + ", discount:" + discount + "}]->(s)";

                    session.run(cypherCreate);

                    j++;

                }

                i++;
                workIndex++;
            }


            workIndex = workIndexOriginal;

            i = 0;
            while (i < workFactor) {

                j = 0;
                while (j < 3) {

                    r = new Random(index);

                    int hours = r.nextInt(100);
                    int worktypeId = j;

                    String sqlInsert = "INSERT INTO warehouse.workhours (worktypeId, hours, discount, workId) VALUES(" + worktypeId + "," + hours + "," + discount + "," + workIndex + ")";

                    workHours.setInt(1, worktypeId);
                    workHours.setInt(2, hours);
                    workHours.setDouble(3, discount);
                    workHours.setInt(4, workIndex);
                    workHours.addBatch();

                    cypherCreate = "MATCH (w:work),(wt:worktype) WHERE w.workId=" + workIndex + " AND wt.worktypeId=" + worktypeId +
                            " CREATE (wt)-[h1:WORKHOURS {hours:" + hours + ", discount:" + discount + "}]->(w)" +
                            " CREATE (w)-[h2:WORKHOURS {hours:" + hours + ", discount:" + discount + "}]->(wt)";
                    //System.out.println(cypherCreate);
                    session.run(cypherCreate);
                    j++;

                }

                i++;
                workIndex++;
            }


        if (index % batchExecuteValue == 0 || index == (iterationCount - 1)) {

            customer.executeBatch();
            invoice.executeBatch();
            target.executeBatch();
            work.executeBatch();
            workTarget.executeBatch();
            workInvoice.executeBatch();
            usedItem.executeBatch();
            workHours.executeBatch();

        }

    }

}