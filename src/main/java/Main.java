import graph.LoadGraph;

public class Main
{
    public static void main(String[] args)
    {
        LoadGraph loadGraph = new LoadGraph();

        loadGraph.getEdges("imdb");

        loadGraph.getNodes("imdb");

        //loadGraph.printNodes();

        loadGraph.getEdgesForNodes("imdb");
        loadGraph.printGraph();

    }
}