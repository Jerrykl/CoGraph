
#include "controller.hpp"
#include "graph.hpp"
#include "engine.hpp"
#include "vertexprogram.hpp"

template<class KeyType, class ValueType>
class KCore: public VertexProgram<KeyType, ValueType>
{
private:
	const ValueType delta = 0;
	typedef Vertex<KeyType, ValueType> VertexType;	
public:
	static int K;
	KCore() {this->iterations = 50; this->acc_init = 0;}
	~KCore() {}
	
	EDGE_DIRECTION gather_edge() {return ALL_EDGES;}
	EDGE_DIRECTION gather_edge(VertexType &v) {return ALL_EDGES;}

	EDGE_DIRECTION scatter_edge() {return ALL_EDGES;}
	EDGE_DIRECTION scatter_edge(VertexType &v)
	{
		// if deleted then scatter
		if(v.change)
			return ALL_EDGES;
		else
			return NO_EDGES;
	}

	ValueType gather(VertexType &v, VertexType &nbr)
	{
		if(nbr.get_value() >= K)
			return 1;
		else
			return 0;
	}

	ValueType op(ValueType x, ValueType y)
	{
		return x + y;
	}

	void apply(VertexType &v, ValueType total)
	{
		// set value to the nubmer of nbrs not deleted
		v.set_value(total);
		v.change = v.get_value() < K ? 1 : 0;

		//log("ID: %d, Value: %d, change: %d\n", v.get_id(), v.get_value(), v.change);
	}

	bool scatter(VertexType &v, VertexType &nbr)
	{
		// printf("nbr: %d\n", nbr.get_value());
		// if nbr not deleted then activate it
		if(nbr.get_value() >= K)
			return true;
		else
			return false;
	}

	static bool init_vertex(VertexType &v)
	{
		v.set_value(K);
		return true;
	}
};

template<class KeyType, class ValueType>
int KCore<KeyType, ValueType>::K = 0;

template<class KeyType, class ValueType>
ValueType op(ValueType x, ValueType y)
{
	return x + y;
}

template<class KeyType>
void format_print(KeyType id, FILE* fp) {
	fprintf(fp, "  %d [front=white, color=red]\n", id);
}

template<class KeyType, class ValueType>
void show_core(Graph<KeyType, ValueType> &graph, ValueType K, const char* outfile) {
	FILE* fp = fopen(outfile, "w+");

	for(auto &pair : graph.low_degree_master) {
		auto &v = pair.second;
		if (v.get_value() >= K) {
			format_print<KeyType>(pair.first, fp);
		}
	}

	for(auto &pair : graph.high_degree_master) {
		auto &v = pair.second;
		if (v.get_value() >= K) {
			format_print<KeyType>(pair.first, fp);
		}
	}

	fclose(fp);
}

int main(int argc, char *argv[])
{
	int opt;
	char *file_path = NULL;
	size_t threshold = 1000;

	char *outfile = NULL;

    while ((opt = getopt(argc, argv, "g:t:k:p:")) != -1) {
        switch (opt) {
        case 'g':
            file_path = optarg;
            break;
        case 't':
            threshold = (size_t)atoi(optarg);
            break;
        case 'k':
        	KCore<int, int>::K = atoi(optarg);
        	break;
        case 'p':
        	outfile = optarg;
        	break;
        default:
            fprintf(stderr, "Usage: %s [-g graph] [-t threshold] [-k K]\n", argv[0]);
            exit(EXIT_FAILURE);
        }
    }

	if (file_path == NULL || KCore<int, int>::K == -1) {
		fprintf(stderr, "Usage: %s [-g graph] [-t threshold] [-k K]\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	Controller<int, int> controller;

	KCore<int, int> kcore;
	
	Graph<int, int> graph(&controller, file_path, threshold, 1);
	graph.transform_vertices(KCore<int, int>::init_vertex);
	
	Engine<int, int> engine(&controller, &graph, &kcore);
	engine.run();

	if (outfile != NULL) {
		show_core<int, int>(graph, KCore<int, int>::K, outfile);
	}
	return 0;
}
