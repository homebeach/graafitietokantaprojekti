import graph.Graph;
import graph.LoadGraph;

public class Main {

    public static void main(String[] args) {

        /*
        Graph graph = new Graph(1);
        graph.loadGraph();
        graph.printGraph();
        */

        LoadGraph graph = new LoadGraph(1);
        graph.getTablesForSchema("imdb");
        graph.printKeysOfTables();
    }
}
