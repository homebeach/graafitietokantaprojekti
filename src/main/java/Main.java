import graph.LoadGraph;

public class Main
{
    public static void main(String[] args)
    {
        LoadGraph loadGraph = new LoadGraph();

        loadGraph.getEdges("imdb");

        loadGraph.printEdges();

    }
}