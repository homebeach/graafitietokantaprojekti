import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class DataGeneratorThreadCustomer extends Thread {

    private static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    private static final String DB_URL = "jdbc:mariadb://127.0.0.1/";

    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    private static final String NEO4J_DB_URL = "bolt://localhost:7687";

    private static final String NEO4J_USERNAME = "neo4j";
    private static final String NEO4J_PASSWORD = "admin";

    private int iterationCount = 0;
    private int batchExecuteValue = 0;
    private int invoiceFactor = 0;
    private int targetFactor = 0;
    private int workFactor = 0;
    private int sequentialInvoices = 0;
    private int workIndex = 0;
    private int workCount = 0;

    private int threadIndex = 0;
    int customerIndex = 0;
    int invoiceIndex = 0;
    int targetIndex = 0;

    int firstnameindex = 0;
    int surnameindex = 0;
    int addressindex = 0;

    private ReentrantLock lock;

    private List<String> firstnames;
    private List<String> surnames;
    private List<HashMap<String, String>> addresses;

    public DataGeneratorThreadCustomer(int threadindex, int iterationCount, int batchExecuteValue, ReentrantLock lock, int invoiceFactor, int targetFactor, int workFactor, int sequentialInvoices, List<String> firstnames, List<String> surnames, List<HashMap<String, String>> addresses, int customerIndex, int invoiceIndex, int targetIndex, int workIndex, int workCount) {

        this.threadIndex = threadindex;
        this.iterationCount = iterationCount;
        this.batchExecuteValue = batchExecuteValue;
        this.invoiceFactor = invoiceFactor;
        this.targetFactor = targetFactor;
        this.workFactor = workFactor;
        this.sequentialInvoices = sequentialInvoices;
        this.firstnames = firstnames;
        this.surnames = surnames;
        this.addresses = addresses;
        this.customerIndex = customerIndex;
        this.invoiceIndex = invoiceIndex;
        this.targetIndex = targetIndex;
        this.workIndex = workIndex;
        this.workCount = workCount;

        this.lock = lock;
    }

    public void run() {

        try {

            org.neo4j.driver.Driver driver = GraphDatabase.driver(NEO4J_DB_URL, AuthTokens.basic(NEO4J_USERNAME, NEO4J_PASSWORD));

            Session session = driver.session();

            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;

            Class.forName(JDBC_DRIVER);

            try (Connection connection = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD)) {

                PreparedStatement customer = connection.prepareStatement("INSERT INTO warehouse.customer (id, name, address) VALUES (?,?,?)");
                PreparedStatement invoice = connection.prepareStatement("INSERT INTO warehouse.invoice (id, customerId, state, duedate, previousinvoice) VALUES (?,?,?,?,?)");
                PreparedStatement workInvoice = connection.prepareStatement("INSERT INTO warehouse.workinvoice (workId, invoiceId) VALUES (?,?)");
                PreparedStatement target = connection.prepareStatement("INSERT INTO warehouse.target (id, name, address, customerid) VALUES (?,?,?,?)");
                PreparedStatement workTarget = connection.prepareStatement("INSERT INTO warehouse.worktarget (workId, targetId) VALUES (?,?)");


                HashMap<String, PreparedStatement> preparedStatements = new HashMap<String, PreparedStatement>();

                preparedStatements.put("customer",customer);
                preparedStatements.put("invoice",invoice);
                preparedStatements.put("target",target);
                preparedStatements.put("workinvoice",workInvoice);
                preparedStatements.put("worktarget",workTarget);

                for (int i = 0; i < iterationCount; i++) {

                    insertRow(i, batchExecuteValue, session, preparedStatements);

                }


            }

            session.close();
            driver.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void setIndexes(int index) {

        Random r = new Random(index);
        firstnameindex = r.nextInt(firstnames.size());
        r = new Random(index);
        surnameindex = r.nextInt(surnames.size());
        r = new Random(index);
        addressindex = r.nextInt(addresses.size());

    }

    public void writeToNeo4J(Session session, String cypherQuery) throws SQLException {

        session.writeTransaction(tx -> tx.run(cypherQuery));

    }

    public List<Integer> getWorkIndexes(int index) {

        Random r = new Random();

        List<Integer> workIndexes = new ArrayList<Integer>();
        for (int i = 0; i < workFactor; i++) {

            r.setSeed(i + index);

            int workIndex = r.nextInt(workCount);

            int offset = 1;
            while (workIndexes.contains(workIndex)) {

                r.setSeed(i + offset);
                workIndex = r.nextInt(workCount);
                offset++;
            }

            workIndexes.add(workIndex);

        }

        return workIndexes;
    }

    public void insertRow(int index, int batchExecuteValue, Session session, HashMap<String, PreparedStatement> preparedStatements) throws SQLException, InterruptedException {

        PreparedStatement customer = preparedStatements.get("customer");
        PreparedStatement invoice = preparedStatements.get("invoice");
        PreparedStatement target = preparedStatements.get("target");
        PreparedStatement workInvoice = preparedStatements.get("workinvoice");
        PreparedStatement workTarget = preparedStatements.get("worktarget");

        int i = 0;
        int j = 0;
        int k = 0;

        System.out.println("Thread: " + threadIndex + " Index: " + index);

        setIndexes(index);

        String name = firstnames.get(firstnameindex) + " " + surnames.get(surnameindex);

        String streetAddress = addresses.get(addressindex).get("street") + " " + addresses.get(addressindex).get("city") + " " + addresses.get(addressindex).get("district") + " " + addresses.get(addressindex).get("region") + " " + addresses.get(addressindex).get("postcode");

        String sqlInsert = "INSERT INTO warehouse.customer (id, name, address) VALUES (" + customerIndex + ",\"" + name + "\",\"" + streetAddress + "\")";

        customer.setInt(1, customerIndex);
        customer.setString(2, name);
        customer.setString(3, streetAddress);
        customer.addBatch();
        customer.executeBatch();
        String cypherCreate = "CREATE (a:customer {customerId: " + customerIndex + ", name:\"" + name + "\",address:\"" + streetAddress + "\"})";
        writeToNeo4J(session, cypherCreate);

        int invoiceIndexOriginal = invoiceIndex;
        int customerInvoiceIndexOriginal = invoiceIndex;

        Random r = new Random(index);

        j = 0;
        while (j < invoiceFactor) {

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
                    invoice.setInt(5, invoiceIndex - 1);
                }

                invoice.addBatch();

                if (j % batchExecuteValue == 0) {

                    invoice.executeBatch();

                }

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
                    writeToNeo4J(session, cypherCreate);

                    cypherCreate = "MATCH (a:customer),(l:invoice) WHERE a.customerId = " + customerIndex + " AND l.invoiceId = " + invoiceIndex + " CREATE (a)-[m:PAYS]->(l)";
                    writeToNeo4J(session, cypherCreate);

                } else {

                    cypherCreate = "CREATE (l:invoice {invoiceId: " + invoiceIndex + ", customerId: " + customerIndex + ", state: " + state + ", duedate: \"date({ year:" + year + ", month:" + month + ", day:" + day + " })\",firstinvoice: " + customerInvoiceIndexOriginal + ", previousinvoice: " + (invoiceIndex - 1) + "})";
                    writeToNeo4J(session, cypherCreate);

                    cypherCreate = "MATCH (a:customer),(l:invoice) WHERE a.customerId = " + customerIndex + " AND l.invoiceId = " + invoiceIndex + " CREATE (a)-[m:PAYS]->(l)";
                    writeToNeo4J(session, cypherCreate);

                    cypherCreate = "MATCH (a:invoice),(b:invoice) WHERE a.invoiceId = " + (invoiceIndex - 1) + " AND b.invoiceId = " + invoiceIndex + " CREATE (a)-[m:PREVIOUS_INVOICE]->(b)";
                    writeToNeo4J(session, cypherCreate);


                }

            } else {

                cypherCreate = "CREATE (l:invoice {invoiceId: " + invoiceIndex + ", customerId: " + customerIndex + ", state: " + state + ", duedate: \"date({ year:" + year + ", month:" + month + ", day:" + day + " })\",firstinvoice: " + invoiceIndex + ", previousinvoice: " + invoiceIndex + "})";
                writeToNeo4J(session, cypherCreate);

                cypherCreate = "MATCH (a:customer),(l:invoice) WHERE a.customerId = " + customerIndex + " AND l.invoiceId = " + invoiceIndex + " CREATE (a)-[m:PAYS]->(l)";
                writeToNeo4J(session, cypherCreate);

            }

            invoiceIndex++;
            j++;
        }

        int targetIndexOriginal = targetIndex;

        j = 0;
        while (j < targetFactor) {

            setIndexes(targetIndex);

            name = firstnames.get(firstnameindex) + " " + surnames.get(surnameindex);

            streetAddress = addresses.get(addressindex).get("street") + " " + addresses.get(addressindex).get("city") + " " + addresses.get(addressindex).get("district") + " " + addresses.get(addressindex).get("region") + " " + addresses.get(addressindex).get("postcode");

            sqlInsert = "INSERT INTO warehouse.target (id, name, address, customerid) VALUES (" + targetIndex + ",\"" + name + "\",\"" + streetAddress + "\"," + customerIndex + ")";

            target.setInt(1, targetIndex);
            target.setString(2, name);
            target.setString(3, streetAddress);
            target.setInt(4, customerIndex);
            target.addBatch();

            cypherCreate = "CREATE (t:target {targetId: " + targetIndex + ", name: \"" + name + "\", address: \"" + streetAddress + "\", customerid: " + customerIndex + " })";
            writeToNeo4J(session, cypherCreate);

            cypherCreate = "MATCH (c:customer),(t:target) WHERE c.customerId = " + customerIndex + " AND t.targetId = " + targetIndex + " CREATE (c)-[m:CUSTOMER_TARGET]->(t)";
            writeToNeo4J(session, cypherCreate);

            targetIndex++;
            j++;

        }

        customerIndex++;

        List<Integer> workIndexes;

        int workIndex = 0;

        targetIndex = targetIndexOriginal;

        i = 0;
        while (i < targetFactor) {

            workIndexes = getWorkIndexes(targetIndex);

            //System.out.println("workIndexes for target: " + targetIndex);
            //System.out.println(workIndexes.toString());

            j = 0;
            while (j < workIndexes.size()) {

                workIndex = workIndexes.get(j);

                sqlInsert = "INSERT INTO warehouse.worktarget (workId, targetId) VALUES (" + workIndex + "," + targetIndex + ")";

                workTarget.setInt(1, workIndex);
                workTarget.setInt(2, targetIndex);
                workTarget.addBatch();

                cypherCreate = "MATCH (s:work),(t:target) WHERE s.workId = " + workIndex + "  AND t.targetId = " + targetIndex + " CREATE (s)-[m:WORK_TARGET]->(t)";

                lock.lock();

                writeToNeo4J(session, cypherCreate);
                //Thread.sleep(500);

                lock.unlock();

                cypherCreate = "MATCH (t:target),(s:work) WHERE t.targetId = " + targetIndex + " AND s.workId = " + workIndex + " CREATE (t)-[m:WORK_TARGET]->(s)";
                writeToNeo4J(session, cypherCreate);


                j++;

            }

        targetIndex++;
        i++;
        }

        invoiceIndex = invoiceIndexOriginal;

        i = 0;
        while (i < invoiceFactor) {

            workIndexes = getWorkIndexes(invoiceIndex);

            //System.out.println("workIndexes for invoice: " + invoiceIndex);
            //System.out.println(workIndexes.toString());

            j = 0;
            while (j < workIndexes.size()) {

                workIndex = workIndexes.get(j);

                r.setSeed(index);

                sqlInsert = "INSERT INTO warehouse.workinvoice (workId, invoiceId) VALUES (" + workIndex + "," + invoiceIndex + ")";

                workInvoice.setInt(1, workIndex);
                workInvoice.setInt(2, invoiceIndex);
                workInvoice.addBatch();

                cypherCreate = "MATCH (s:work),(l:invoice) WHERE s.workId = " + workIndex + " AND l.invoiceId = " + invoiceIndex + " CREATE (s)-[m:WORK_INVOICE]->(l)";
                writeToNeo4J(session, cypherCreate);


                cypherCreate = "MATCH (l:invoice), (s:work) WHERE l.invoiceId = " + invoiceIndex + " AND s.workId = " + workIndex + "  CREATE (l)-[m:WORK_INVOICE]->(s)";
                writeToNeo4J(session, cypherCreate);

                j++;

            }

            invoiceIndex++;
            i++;
        }

        r.setSeed(index);
        int discountPercent = 1 + r.nextInt(101);
        double discount = (0.01 * discountPercent);

        workIndex++;

        if (index % batchExecuteValue == 0 || index == (iterationCount - 1)) {

            customer.executeBatch();
            invoice.executeBatch();
            target.executeBatch();
            workTarget.executeBatch();
            workInvoice.executeBatch();

        }

    }

}