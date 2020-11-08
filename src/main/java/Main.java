import java.util.HashMap;

public class Main
{
    public static void main(String[] args)
    {

        HashMap<String, String[]> sql_databases = new HashMap<String, String[]>();

        String db_mariadb_url = "jdbc:mariadb://127.0.0.1:3306/";
        String db_driver = "org.mariadb.jdbc.Driver";
        String db_username = "root";
        String db_password = "root";

        String[] db_settings = new String[3];

        db_settings[0] = db_driver;
        db_settings[1] = db_username;
        db_settings[2] = db_password;

        sql_databases.put(db_mariadb_url, db_settings);

        String mysql_db_url = "jdbc:mysql://127.0.0.1:3307/";
        db_driver = "org.mariadb.jdbc.Driver";
        db_username = "root";
        db_password = "root";

        db_settings = new String[3];

        db_settings[0] = db_driver;
        db_settings[1] = db_username;
        db_settings[2] = db_password;

        sql_databases.put(mysql_db_url, db_settings);

        HashMap<String, String> neo4j_settings = new HashMap<String, String>();

        String neo4J_db_url = "bolt://localhost:7687";
        String neo4J_username = "neo4j";
        String neo4j_password = "admin";

        neo4j_settings.put("NEO4J_DB_URL", neo4J_db_url);
        neo4j_settings.put("NEO4J_USERNAME", neo4J_username);
        neo4j_settings.put("NEO4J_PASSWORD", neo4j_password);

        DataGenerator dataGenerator = new DataGenerator(sql_databases, neo4j_settings, db_mariadb_url);

        //dataGenerator.createTables();

        //dataGenerator.getSampleData();

        //dataGenerator.insertItemsAndWorkTypes(10, 10, 10000, 10000);

        //dataGenerator.createSampleTables("jdbc:mariadb://127.0.0.1:3306/");

        //dataGenerator.loadSampleData(10, "jdbc:mariadb://127.0.0.1:3306/");

        //dataGenerator.printSampleDataSizes();

        //dataGenerator.insertWorkData(5,1000,10,10,10);

        //dataGenerator.insertCustomerData(5,1000,10,10,0,10,10);

        //dataGenerator.insertCustomerData(1,149,10,10,10,10,10);

        QueryTester queryTester = new QueryTester(sql_databases, neo4j_settings);
        //dataGenerator.cleanSequentialInvoices(10000);
        //dataGenerator.deleteIndexes();
        //dataGenerator.createIndexes();
        //queryTester.executeQueryTestsSQL(12, true);

        dataGenerator.createIndexes();

        queryTester.executeQueryTestsCypher(12, true);

        System.out.println();
        System.out.println("Deleting indexes");
        System.out.println();

        dataGenerator.deleteIndexes();

        queryTester.executeQueryTestsCypher(12, true);

        //queryTester.executeAggregateQueryTestCypher(12, true);
        //queryTester.executeAggregateQueryTestSQL(12, true);
        //queryTester.executeQueryWithDefinedKeyCypher(12, true);
        //queryTester.executeQueryWithDefinedKeySQL(12, true);
        //dataGenerator.cleanSequentialInvoices(10000);



        /*
        HashMap<String, Integer> customerInvoice =  dataGenerator.insertSequentialInvoices(1,10,100);

        int invoiceIndex = customerInvoice.get("invoiceIndex");
        int customerIndex = customerInvoice.get("customerIndex");

        queryTester.executeCyclicQueryTestSQL(12, true, invoiceIndex);
        //queryTester.executeOptimizedRecursiveQueryTest(12, true, invoiceIndex);
        //queryTester.executeRecursiveQueryTestCypher(12, true, invoiceIndex);

        System.out.println("customerIndex " + customerIndex);
        dataGenerator.cleanSequentialInvoices(customerIndex);

        customerInvoice =  dataGenerator.insertSequentialInvoices(1,10,1000);

        invoiceIndex = customerInvoice.get("invoiceIndex");

        queryTester.executeCyclicQueryTestSQL(12, true, invoiceIndex);
        //queryTester.executeOptimizedRecursiveQueryTest(12, true, invoiceIndex);
        //queryTester.executeRecursiveQueryTestCypher(12, true, invoiceIndex);
        */
        //dataGenerator.cleanSequentialInvoices(10000);

        //queryTester.executeRecursiveQueryTest(12, true, 100000);


      //  int customerIndex = customerInvoice.get("customerIndex");

        //dataGenerator.cleanSequentialInvoices(10000);

//      firstInvoiceIndex = dataGenerator.insertSequentialInvoices(10,10,10, 1000);

//      queryTester.executeRecursiveQueryTest(10, true, firstInvoiceIndex);

        //TinkerPopGraph tinkerPopGraph = new TinkerPopGraph();

        //tinkerPopGraph.getVertexes("varasto");

        //tinkerPopGraph.printVertexKeys();

        //tinkerPopGraph.printPrimaryKeysValues();

        //System.out.println("tinkerPopGraph vertexes length: " + tinkerPopGraph.getVertexes().size());

        //tinkerPopGraph.getEdges("varasto");

        //tinkerPopGraph.printVertexes();

        //tinkerPopGraph.printRealPrimaryKeysValues();


        //Convert convert = new Convert();

        //String cypherQuery = convert.getCypher("*");

        //System.out.println(cypherQuery);

        //String matchAll = "MATCH (n) RETURN n;";

        //List<Map<String, Object>> results = tinkerPopGraph.executeCypherQuery(cypherQuery);

        //System.out.println(results.size());

        /*

        for (Map<String, Object> queryMember : results) {

            Set<String> keys = queryMember.keySet();

            for (String key : keys) {

                Object member = queryMember.get(key);
                System.out.println(member.toString());

            }

        }

        */

        /*

        tinkerPopGraph.printEdges();

        tinkerPopGraph.printNodes();

        tinkerPopGraph.getEdgesForNodes("varasto");

        tinkerPopGraph.printGraph();

        */
    }
}