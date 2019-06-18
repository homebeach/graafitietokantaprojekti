import graph.Graph;

public class Main
{
    public static void main(String[] args)
    {

        DataGeneratorSQL dataGeneratorSQL = new DataGeneratorSQL();

        dataGeneratorSQL.insertData(100);

        TinkerPopGraph tinkerPopGraph = new TinkerPopGraph();

        tinkerPopGraph.getVertexes("varasto");

        System.out.println("tinkerPopGraph vertexes length: " + tinkerPopGraph.getVertexes().size());

        tinkerPopGraph.getEdges("varasto");


        /*

        tinkerPopGraph.printEdges();

        tinkerPopGraph.printNodes();

        tinkerPopGraph.getEdgesForNodes("varasto");

        tinkerPopGraph.printGraph();

        */
    }
}