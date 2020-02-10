import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.sql.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class DataGeneratorThreadWork extends Thread {

    private static final String NEO4J_DB_URL = "bolt://localhost:7687";

    private static final String NEO4J_USERNAME = "neo4j";
    private static final String NEO4J_PASSWORD = "admin";

    private HashMap<String, String[]> sql_databases;

    private int iterationCount = 0;
    private int batchExecuteValue = 0;

    private int threadIndex = 0;

    int workIndex = 0;
    int itemFactor = 0;
    int itemCount = 0;
    int workTypeFactor = 0;
    int workTypeCount = 0;

    private ReentrantLock lock;

    public DataGeneratorThreadWork(int threadIndex, int iterationCount, int batchExecuteValue, HashMap<String, String[]> sql_databases, ReentrantLock lock, int workIndex, int itemFactor, int itemCount, int workTypeFactor, int workTypeCount) {

        this.threadIndex = threadIndex;
        this.iterationCount = iterationCount;
        this.batchExecuteValue = batchExecuteValue;
        this.workIndex = workIndex;
        this.itemFactor = itemFactor;
        this.itemCount = itemCount;
        this.workTypeFactor = workTypeFactor;
        this.workTypeCount = workTypeCount;
        this.sql_databases = sql_databases;
        this.lock = lock;
    }

    public void run() {

        try {

            org.neo4j.driver.Driver driver = GraphDatabase.driver(NEO4J_DB_URL, AuthTokens.basic(NEO4J_USERNAME, NEO4J_PASSWORD));

            Session session = driver.session();

            Connection conn = null;
            Statement stmt = null;
            ResultSet resultSet = null;

            List<HashMap> preparedStatementsList = new ArrayList();

            List<Connection> connectionList = new ArrayList();

            for (String db_url : sql_databases.keySet()) {

                String[] db_info = sql_databases.get(db_url);

                String db_driver = db_info[0];
                String db_username = db_info[1];
                String db_password = db_info[2];

                Class.forName(db_driver);

                Connection connection = DriverManager.getConnection(db_url, db_username, db_password);
                connectionList.add(connection);

                PreparedStatement work = connection.prepareStatement("INSERT INTO warehouse.work (id, name) VALUES (?,?)");
                PreparedStatement usedItem = connection.prepareStatement("INSERT INTO warehouse.useditem (amount, discount, workId, warehouseitemId) VALUES(?,?,?,?)");
                PreparedStatement workHours = connection.prepareStatement("INSERT INTO warehouse.workhours (worktypeId, hours, discount, workId) VALUES(?,?,?,?)");

                HashMap<String, PreparedStatement> preparedStatements = new HashMap<String, PreparedStatement>();

                preparedStatements.put("work", work);
                preparedStatements.put("useditem", usedItem);
                preparedStatements.put("workhours", workHours);

                preparedStatementsList.add(preparedStatements);

            }

            for (int i = 0; i < iterationCount; i++) {

                insertWork(i, batchExecuteValue, session, preparedStatementsList);

            }

            for (Connection connection : connectionList) {
                connection.close();
            }

            session.close();
            driver.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    public void writeToNeo4J(Session session, String cypherQuery) throws SQLException {

        session.writeTransaction(tx -> tx.run(cypherQuery));

    }

    public List<Integer> getItemIndexes(int index) {

        Random r = new Random();

        int itemSeed = 10;

        List<Integer> itemIndexes = new ArrayList<Integer>();
        for(int i=0; i<itemFactor; i++) {

            r.setSeed(i + index + itemSeed);

            int itemIndex = r.nextInt(itemCount);

            int offset = 1;
            while(itemIndexes.contains(itemIndex)) {
                r.setSeed(i + index + itemSeed + offset);
                itemIndex = r.nextInt(itemCount);
                offset++;
            }

            itemIndexes.add(itemIndex);

        }

    return itemIndexes;
    }

    public List<Integer> getWorkTypeIndexes(int index) {

        Random r = new Random();

        int workTypeSeed = 20;

        List<Integer> workTypeIndexes = new ArrayList<Integer>();
        for (int i = 0; i < workTypeFactor; i++) {

            r.setSeed(i + index + workTypeSeed);

            int workTypeIndex = r.nextInt(workTypeCount);

            int offset = 1;
            while (workTypeIndexes.contains(workTypeIndex)) {

                r.setSeed(i + index + workTypeSeed + offset);
                workTypeIndex = r.nextInt(workTypeCount);
                offset++;
            }

            workTypeIndexes.add(workTypeIndex);

        }

        return workTypeIndexes;
    }


    public void insertWork(int index, int batchExecuteValue, Session session, List<HashMap> preparedStatementsList) throws SQLException, InterruptedException {

        PreparedStatement work;
        PreparedStatement usedItem;
        PreparedStatement workHours;

        System.out.println("Thread: " + threadIndex + " Index: " + index);

        int workIndexOriginal = workIndex;

        String workName = "Generic " + workIndex;

        String sqlInsert = "INSERT INTO warehouse.work (id, name) VALUES (" + workIndex + ",'" + workName + "')";

        for (HashMap<String, PreparedStatement> preparedStatements : preparedStatementsList) {

            work = preparedStatements.get("work");

            work.setInt(1, workIndex);
            work.setString(2, workName);
            work.addBatch();

        }

        String cypherCreate = "CREATE (s:work {workId: " + workIndex + ", name: \"" + workName + "\"})";
        writeToNeo4J(session, cypherCreate);

        Random r = new Random(index);

        int discountPercent = 1 + r.nextInt(101);
        double discount = (0.01 * discountPercent);

        List<Integer> itemIndexes = getItemIndexes(workIndex);

        //System.out.println("itemIndexes");
        //System.out.println(itemIndexes.toString());

        int i = 0;
        while (i < itemIndexes.size()) {

            r.setSeed(index);

            int amount = 1 + r.nextInt(101);

            int wareHouseItemId = itemIndexes.get(i);

            //sqlInsert = "INSERT INTO warehouse.useditem (amount, discount, workId, warehouseitemId) VALUES(" + amount + "," + discount + "," + workIndex + "," + warehouseitemId + ")";

            for (HashMap<String, PreparedStatement> preparedStatements : preparedStatementsList) {

                usedItem = preparedStatements.get("useditem");

                usedItem.setInt(1, amount);
                usedItem.setDouble(2, discount);
                usedItem.setInt(3, workIndex);
                usedItem.setInt(4, wareHouseItemId);
                usedItem.addBatch();

            }

            cypherCreate = "MATCH (s:work),(v:warehouseitem) WHERE s.workId=" + workIndex + " AND v.warehouseitemId=" + wareHouseItemId + " CREATE (s)-[ui:USED_ITEM {amount:" + amount + ", discount:" + discount + "}]->(v) ";
            writeToNeo4J(session, cypherCreate);

            cypherCreate = "MATCH (v:warehouseitem),(s:work) WHERE v.warehouseitemId=" + wareHouseItemId + " AND s.workId=" + workIndex + "   CREATE (v)-[ui:USED_ITEM {amount:" + amount + ", discount:" + discount + "}]->(s)";
            writeToNeo4J(session, cypherCreate);

            i++;

        }

        List<Integer> workTypeIndexes =  getWorkTypeIndexes(workIndex);

        //System.out.println("workTypeIndexes");
        //System.out.println(workTypeIndexes.toString());

        i = 0;
        while (i < workTypeIndexes.size()) {

            r.setSeed(index);

            int hours = r.nextInt(100);
            int worktypeId = workTypeIndexes.get(i);

            sqlInsert = "INSERT INTO warehouse.workhours (worktypeId, hours, discount, workId) VALUES(" + worktypeId + "," + hours + "," + discount + "," + workIndex + ")";

            for (HashMap<String, PreparedStatement> preparedStatements : preparedStatementsList) {

                workHours = preparedStatements.get("workhours");

                workHours.setInt(1, worktypeId);
                workHours.setInt(2, hours);
                workHours.setDouble(3, discount);
                workHours.setInt(4, workIndex);
                workHours.addBatch();

            }

            cypherCreate = "MATCH (w:work),(wt:worktype) WHERE w.workId=" + workIndex + " AND wt.worktypeId=" + worktypeId + " CREATE (w)-[wh:WORKHOURS {hours:" + hours + ", discount:" + discount + "}]->(wt) ";
            writeToNeo4J(session, cypherCreate);

            cypherCreate = "MATCH (wt:worktype),(w:work) WHERE wt.worktypeId=" + worktypeId + " AND w.workId=" + workIndex  + " CREATE (wt)-[wh:WORKHOURS {hours:" + hours + ", discount:" + discount + "}]->(w)";
            writeToNeo4J(session, cypherCreate);




            //lock.lock();


            //Thread.sleep(500);
            //lock.unlock();
            i++;

        }

        workIndex++;

        if (index % batchExecuteValue == 0 || index == (iterationCount - 1)) {

            for (HashMap<String, PreparedStatement> preparedStatements : preparedStatementsList) {

                work = preparedStatements.get("work");
                usedItem = preparedStatements.get("useditem");
                workHours = preparedStatements.get("workhours");

                work.executeBatch();
                usedItem.executeBatch();
                workHours.executeBatch();

            }

        }

    }

}