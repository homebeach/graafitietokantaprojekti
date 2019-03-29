CREATE OR REPLACE DATABASE graph;

CREATE TABLE graph.nodes (
	node_id INT,
	node_content JSON,
	graph_id INT,
    PRIMARY KEY (node_id)
)COLLATE='utf8_bin';

CREATE TABLE graph.edges (
	edge_id INT,
	from_node_id INT,
	to_node_id INT,
	edge_content JSON,
	graph_id INT,
    PRIMARY KEY (edge_id)
)COLLATE='utf8_bin';

