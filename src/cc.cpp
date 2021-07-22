
#include "controller.hpp"
#include "graph.hpp"
#include "engine.hpp"
#include "vertexprogram.hpp"
#include <algorithm>
#include <math.h>
#include <unordered_map>

template<class KeyType, class ValueType>
class ConnectedComponent: public VertexProgram<KeyType, ValueType>
{
private:
	ValueType delta = 0;
	//ValueType change;
	typedef Vertex<KeyType, ValueType> VertexType;
public:
	ConnectedComponent() {this->iterations = 32; this->acc_init = 1 << 30;}
	~ConnectedComponent() {}

	EDGE_DIRECTION gather_edge() {return ALL_EDGES;}
	EDGE_DIRECTION gather_edge(VertexType &v) {return ALL_EDGES;}
	EDGE_DIRECTION scatter_edge() {return ALL_EDGES;}
	EDGE_DIRECTION scatter_edge(VertexType &v)
	{
		if(v.change)
			return ALL_EDGES;
		else
			return NO_EDGES;
	}

	ValueType gather(VertexType &v, VertexType &nbr)
	{
		return nbr.get_value();
	}

	ValueType op(ValueType x, ValueType y)
	{
		return std::min(x, y);
	}

	void apply(VertexType &v, ValueType total)
	{
		ValueType old_value = v.get_value();
		v.set_value(std::min(old_value, total));
		v.change = v.get_value() - old_value;
	}

	bool scatter(VertexType &v, VertexType &nbr)
	{
		if(v.get_value() < nbr.get_value())
			return true;
		else
			return false;
	}
	
	static bool init_vertex(VertexType &v)
	{
		v.set_value(v.get_id());
		return true;
	}

};

template<class KeyType, class ValueType>
ValueType op(ValueType x, ValueType y)
{
	return std::min(x, y);
}

// 
// use the link below to visualize the result
// https://visjs.github.io/vis-network/examples/network/data/dotLanguage/dotPlayground.html
// 
template<class KeyType, class ValueType>
void format_print(Vertex<KeyType, ValueType> &v, FILE* fp) {
	static std::vector<const char*> colors = {"red", "orange", "yellow", "green", "cyan", "blue", "purple", "black"};
	static size_t idx = 0;
	static std::unordered_map<ValueType, size_t> um;

	if (um.count(v.get_value()) == 0) {
		if (idx >= colors.size()) {
			printf_red("Colors not enough: %ld\n", colors.size());
			exit(0);
		}
		um[v.get_value()] = idx++;
	}

	fprintf(fp, "  %d [front=white, color=%s]\n", v.get_id(), colors[um[v.get_value()]]);
}

template<class KeyType, class ValueType>
void show_cc(Graph<KeyType, ValueType> &graph, const char* outfile) {

	FILE* fp = fopen(outfile, "w+");

	for(auto &pair : graph.low_degree_master) {
		format_print(pair.second, fp);
	}

	for(auto &pair : graph.high_degree_master) {
		format_print(pair.second, fp);
	}

	fclose(fp);
}

int main(int argc, char *argv[])
{
	int opt;
	char *file_path = NULL;
	size_t threshold = 1000;

	char *outfile = NULL;

    while ((opt = getopt(argc, argv, "g:t:p:")) != -1) {
        switch (opt) {
        case 'g':
            file_path = optarg;
            break;
        case 't':
            threshold = (size_t)atoi(optarg);
            break;
        case 'p':
        	outfile = optarg;
        	break;
        default:
            fprintf(stderr, "Usage: %s [-g graph] [-t threshold]\n", argv[0]);
            exit(EXIT_FAILURE);
        }
    }

	if (file_path == NULL) {
		fprintf(stderr, "Usage: %s [-g graph] [-t threshold]\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	Controller<unsigned int, unsigned int> controller;

	ConnectedComponent<unsigned int, unsigned int> connectedcomponent;
	
	Graph<unsigned int, unsigned int> graph(&controller, file_path, threshold, 1);
	graph.transform_vertices(ConnectedComponent<unsigned int, unsigned int>::init_vertex);
	
	Engine<unsigned int, unsigned int> engine(&controller, &graph, &connectedcomponent);
	engine.run();

	if (outfile != NULL) {
		show_cc<unsigned int, unsigned int>(graph, outfile);
	}

	return 0;
}
