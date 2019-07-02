import graph.Graph;

public class Main
{
    public static void main(String[] args)
    {

        DataGeneratorSQL dataGeneratorSQL = new DataGeneratorSQL();

        dataGeneratorSQL.insertData(100);

        TinkerPopGraph tinkerPopGraph = new TinkerPopGraph();

        tinkerPopGraph.getVertexes("varasto");

        //tinkerPopGraph.printVertexKeys();

        //tinkerPopGraph.printPrimaryKeysValues();

        //System.out.println("tinkerPopGraph vertexes length: " + tinkerPopGraph.getVertexes().size());

        tinkerPopGraph.getEdges("varasto");

        //tinkerPopGraph.printRealPrimaryKeysValues();

        /*

        tinkerPopGraph.printEdges();

        tinkerPopGraph.printNodes();

        tinkerPopGraph.getEdgesForNodes("varasto");

        tinkerPopGraph.printGraph();

        */
    }
}