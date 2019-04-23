import graph.Graph;

public class Main
{
    public static void main(String[] args)
    {
        Graph graph = new Graph();

        graph.getEdges("varasto");

        //graph.printEdges();

        graph.getNodes("varasto");

        //graph.printEdges();

        //graph.printNodes();

        graph.getEdgesForNodes("varasto");

        graph.printGraph();

    }
}