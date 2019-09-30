public class Main
{
    public static void main(String[] args)
    {

        DataGenerator dataGenerator = new DataGenerator();

        dataGenerator.getSampleData();

        dataGenerator.insertData(10, 5,5,4,5,5,5);

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