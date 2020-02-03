import java.util.Random;

public class Main
{
    public static void main(String[] args)
    {

        DataGenerator dataGenerator = new DataGenerator();

        //dataGenerator.createTables();


        dataGenerator.truncateDatabase();

        //dataGenerator.getSampleData();

        dataGenerator.createItems(100);
        dataGenerator.createWorkTypes(100);

        /*

        dataGenerator.createTestTables();

        dataGenerator.loadTestData(50);

         */

        //dataGenerator.printSampleDataSizes();



        dataGenerator.insertWorkData(1,1,1,10,10,10);

        dataGenerator.insertCustomerData(1,1,1,100,100,10,10);


        QueryTester queryTester = new QueryTester();

        queryTester.executeQueryTests(10, true);

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