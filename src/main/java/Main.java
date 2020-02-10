import java.util.HashMap;
import java.util.Random;

public class Main
{
    public static void main(String[] args)
    {


        HashMap<String, String[]> sql_databases = new HashMap<String, String[]>();

        String db_url = "jdbc:mariadb://127.0.0.1:3306/";
        String db_driver = "org.mariadb.jdbc.Driver";
        String db_username = "root";
        String db_password = "root";

        String[] db_info = new String[3];

        db_info[0] = db_driver;
        db_info[1] = db_username;
        db_info[2] = db_password;

        sql_databases.put(db_url, db_info);

        db_url = "jdbc:mysql://127.0.0.1:3307/";
        db_driver = "com.mysql.jdbc.Driver";
        db_username = "root";
        db_password = "root";

        db_info = new String[3];

        db_info[0] = db_driver;
        db_info[1] = db_username;
        db_info[2] = db_password;

        sql_databases.put(db_url, db_info);


        DataGenerator dataGenerator = new DataGenerator(sql_databases);

        dataGenerator.createTables();


        dataGenerator.truncateDatabases();

        //dataGenerator.getSampleData();

        dataGenerator.insertItemsAndWorkTypes(10, 10, 10, 10);



        /*

        dataGenerator.createTestTables();

        dataGenerator.loadTestData(50);

         */

        //dataGenerator.printSampleDataSizes();



        dataGenerator.insertWorkData(10,10,10,10,10,10);

        dataGenerator.insertCustomerData(10,10,10,10,10,10,10);


        //QueryTester queryTester = new QueryTester();

        //queryTester.executeQueryTests(4, true);

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

        -katsotaan Neo4J ajonaaikaiset parametrit kuntoon
        -asenna mysql ja tee sama testi mysql kanssa yritä etsiä sama versio kuin intialaisten tutkimuksessa
        -töiden summa asiakkaalle x
        -vie 100 000 riviä

        -kokeillaan a->b->c relaatio asiakkaalle x 100 000 rivillä


        */
    }
}