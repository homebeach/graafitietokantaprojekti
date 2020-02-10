import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

public class DataGeneratorThreadItemsAndWorkTypes extends Thread {

    private static final String NEO4J_DB_URL = "bolt://localhost:7687";

    private static final String NEO4J_USERNAME = "neo4j";
    private static final String NEO4J_PASSWORD = "admin";

    private HashMap<String, String[]> sql_databases;

    private int batchExecuteValue = 0;

    private int threadIndex = 0;

    int itemIndex = 0;
    int workTypeIndex = 0;

    int itemCount = 0;
    int workTypeCount = 0;

    private ReentrantLock lock;

    public DataGeneratorThreadItemsAndWorkTypes(int threadIndex, int batchExecuteValue, HashMap<String, String[]> sql_databases, ReentrantLock lock, int itemIndex, int itemCount, int workTypeIndex, int workTypeCount) {

        this.threadIndex = threadIndex;
        this.batchExecuteValue = batchExecuteValue;
        this.itemIndex = itemIndex;
        this.itemCount = itemCount;
        this.workTypeIndex = workTypeIndex;
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

                PreparedStatement warehouseItem = connection.prepareStatement("INSERT INTO warehouse.warehouseitem (id, name, balance, unit, purchaseprice, vat, removed) VALUES (?,?,?,?,?,?,?)");
                PreparedStatement workType = connection.prepareStatement("INSERT INTO warehouse.worktype (id, name, price) VALUES (?, ?, ?)");

                HashMap<String, PreparedStatement> preparedStatements = new HashMap<String, PreparedStatement>();

                preparedStatements.put("warehouseitem", warehouseItem);
                preparedStatements.put("worktype", workType);

                preparedStatementsList.add(preparedStatements);

            }

            insert(batchExecuteValue, session, preparedStatementsList);

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


    public void insert(int batchExecuteValue, Session session, List<HashMap> preparedStatementsList) throws SQLException, InterruptedException {


        PreparedStatement warehouseItem;

        for (int i = 0; i < itemCount; i++) {

            System.out.println("threadIndex: " + threadIndex + " itemIndex: " + itemIndex);

            Random r = new Random(itemIndex);
            int balance = r.nextInt(100);
            r.setSeed(itemIndex);
            float purchaseprice = r.nextFloat();
            r.setSeed(itemIndex);
            int vat = r.nextInt(50);
            r.setSeed(itemIndex);
            boolean removed = r.nextBoolean();

            if (i % 2 == 0) {

                r.setSeed(itemIndex);
                r = new Random(itemIndex);
                int x = r.nextInt(10);
                r.setSeed(itemIndex + 1);
                int y = r.nextInt(10);
                r.setSeed(itemIndex + 2);
                int size = r.nextInt(10);

                String item = "MMJ " + x + "X" + y + "," + size + "MM²CABLE";

                for (HashMap<String, PreparedStatement> preparedStatements : preparedStatementsList) {

                    warehouseItem = preparedStatements.get("warehouseitem");

                    warehouseItem.setInt(1, itemIndex);
                    warehouseItem.setString(2, item);
                    warehouseItem.setInt(3, balance);
                    warehouseItem.setString(4, "m");
                    warehouseItem.setFloat(5, purchaseprice);
                    warehouseItem.setInt(6, vat);
                    warehouseItem.setBoolean(7, removed);
                    warehouseItem.addBatch();

                }

                session.run("CREATE (v:warehouseitem {warehouseitemId: " + itemIndex + ", name: \"" + item + "\", balance:" + balance + ", unit:\"m\", purchaseprice:" + purchaseprice + ", vat:" + vat + ", removed:" + removed + "})");


            } else if (i % 3 == 0) {

                r.setSeed(itemIndex);
                int ground = r.nextInt(10);
                String item = "SOCKET " + ground + "-GROUND OL JUSSI";

                for (HashMap<String, PreparedStatement> preparedStatements : preparedStatementsList) {

                    warehouseItem = preparedStatements.get("warehouseitem");

                    warehouseItem.setInt(1, itemIndex);
                    warehouseItem.setString(2, item);
                    warehouseItem.setInt(3, balance);
                    warehouseItem.setString(4, "pcs");
                    warehouseItem.setFloat(5, purchaseprice);
                    warehouseItem.setInt(6, vat);
                    warehouseItem.setBoolean(7, removed);
                    warehouseItem.addBatch();

                }

                session.run("CREATE (v:warehouseitem {warehouseitemId: " + itemIndex + ", name:\"" + item + "\", balance:" + balance + ", unit:\"pcs\", purchaseprice:" + purchaseprice + ", vat:" + vat + ", removed:" + removed + "})");

            } else if (i % 5 == 0) {

                r.setSeed(itemIndex);
                int spiral1 = r.nextInt(10);
                r.setSeed(itemIndex + 1);
                int spiral2 = r.nextInt(10);
                r.setSeed(itemIndex + 2);
                int spiral3 = r.nextInt(100);

                String item = "BINDING SPIRAL " + spiral1 + "," + spiral2 + "-" + spiral3 + "MM INVISIBLE";

                for (HashMap<String, PreparedStatement> preparedStatements : preparedStatementsList) {

                    warehouseItem = preparedStatements.get("warehouseitem");

                    warehouseItem.setInt(1, itemIndex);
                    warehouseItem.setString(2, item);
                    warehouseItem.setInt(3, balance);
                    warehouseItem.setString(4, "pcs");
                    warehouseItem.setFloat(5, purchaseprice);
                    warehouseItem.setInt(6, vat);
                    warehouseItem.setBoolean(7, removed);
                    warehouseItem.addBatch();

                }

                session.run("CREATE (v:warehouseitem {warehouseitemId: " + itemIndex + ", name:\"" + item + "\", balance:" + balance + ", unit:\"pcs\", purchaseprice:" + purchaseprice + ", vat:" + vat + ", removed:" + removed + "})");


            } else {

                r.setSeed(itemIndex);
                int parts = r.nextInt(10);

                String item = "SOCKET CORNER MODEL " + parts + "-PARTS";

                for (HashMap<String, PreparedStatement> preparedStatements : preparedStatementsList) {

                    warehouseItem = preparedStatements.get("warehouseitem");

                    warehouseItem.setInt(1, itemIndex);
                    warehouseItem.setString(2, item);
                    warehouseItem.setInt(3, balance);
                    warehouseItem.setString(4, "'pcs'");
                    warehouseItem.setFloat(5, purchaseprice);
                    warehouseItem.setInt(6, vat);
                    warehouseItem.setBoolean(7, removed);
                    warehouseItem.addBatch();

                }

                session.run("CREATE (v:warehouseitem {warehouseitemId: " + itemIndex + ", name:\"" + item + "\", balance:" + balance + ", unit:\"pcs\", purchaseprice:" + purchaseprice + ", vat:" + vat + ", removed:" + removed + "})");

            }

            if (i % batchExecuteValue == 0 || i == (itemCount - 1)) {

                for (HashMap<String, PreparedStatement> preparedStatements : preparedStatementsList) {

                    warehouseItem = preparedStatements.get("warehouseitem");
                    warehouseItem.executeBatch();

                }

            }

            itemIndex++;

        }

        PreparedStatement workType;

        for (int i = 0; i < workTypeCount; i++) {

            Random r = new Random(workTypeIndex);
            int price = r.nextInt(100);

            for (HashMap<String, PreparedStatement> preparedStatements : preparedStatementsList) {

                workType = preparedStatements.get("worktype");
                workType.setInt(1, workTypeIndex);
                workType.setInt(3, price);

                if (i % 2 == 0) {

                    workType.setString(2, "design");
                    session.run("CREATE (wt:worktype {worktypeId: " + workTypeIndex + ", name:\"design\", price:" + price + "})");

                } else if (i % 3 == 0) {

                    workType.setString(2, "work");
                    session.run("CREATE (wt:worktype {worktypeId: " + workTypeIndex + ", name:\"work\", price:" + price + "})");

                } else {

                    workType.setString(2, "supporting work");
                    session.run("CREATE (wt:worktype {worktypeId: " + workTypeIndex + ", name:\"supporting work\", price:" + price + "})");

                }

                workType.addBatch();

            }

            if (i % batchExecuteValue == 0 || i == (workTypeCount - 1)) {

                for (HashMap<String, PreparedStatement> preparedStatements : preparedStatementsList) {

                    warehouseItem = preparedStatements.get("worktype");
                    warehouseItem.executeBatch();

                }

            }

            workTypeIndex++;

        }

    }

}