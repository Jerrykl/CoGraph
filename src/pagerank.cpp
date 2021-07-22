
#include "controller.hpp"
#include "graph.hpp"
#include "engine.hpp"
#include "vertexprogram.hpp"

template<class KeyType, class ValueType>
class PageRank: public VertexProgram<KeyType, ValueType>
{
private:
	ValueType delta = 1.0E-9;
	//ValueType change;
	const double res_prob = 0.85;
	typedef Vertex<KeyType, ValueType> VertexType;
public:
	PageRank() {this->iterations = 32; this->acc_init = 0;}
	~PageRank() {}

	EDGE_DIRECTION gather_edge() {return IN_EDGES;}
	EDGE_DIRECTION gather_edge(VertexType &v) {return IN_EDGES;}

	EDGE_DIRECTION scatter_edge() {return OUT_EDGES;}
	EDGE_DIRECTION scatter_edge(VertexType &v)
	{
		// if(v.change > delta || v.change < -delta)
		// {
			// log("Note:ID:%d, change:%.10lf, delta:%.10lf\n", v.get_id(), v.change, delta);
			return OUT_EDGES;
		// }
		// else
			// return NO_EDGES;
	}

	ValueType gather(VertexType &v, VertexType &nbr)
	{
		return nbr.get_value() / nbr.out_nbrs_v.size();
	}

	ValueType op(ValueType x, ValueType y)
	{
		return x + y;
	}

	void apply(VertexType &v, ValueType total)
	{
		ValueType old_value = v.get_value();
		v.set_value((1 - res_prob) * total + res_prob);
		v.change = v.get_value() - old_value;
		// if(v.change <= delta && v.change >= -delta)
			v.change = 1;
		// log("INFO:id:%d, new value:%lf, old_value:%lf\n", v.get_id(), v.get_value(), old_value);
	}

	bool scatter(VertexType &v, VertexType &nbr)
	{
		return true;
	}

	static bool init_vertex(VertexType &v)
	{
		v.set_value(1);
		return true;
	}

};

template<class KeyType, class ValueType>
ValueType op(ValueType x, ValueType y)
{
	return x + y;
}

int main(int argc, char *argv[])
{
	int opt;
	char *file_path = NULL;
	size_t threshold = 1000;

    while ((opt = getopt(argc, argv, "g:t:")) != -1) {
        switch (opt) {
        case 'g':
            file_path = optarg;
            break;
        case 't':
            threshold = (size_t)atoi(optarg);
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

	Controller<unsigned int, double> controller;

	// volatile int set_break = 0;
	// if(controller.comm->get_rank() != 0)
	// 	set_break = 0;
	// while(set_break);

	PageRank<unsigned int, double> pagerank;
	Graph<unsigned int, double> graph(&controller, file_path, threshold, 1);
	graph.transform_vertices(PageRank<unsigned int, double>::init_vertex);
	Engine<unsigned int, double> engine(&controller, &graph, &pagerank);
	engine.run();
	return 0;
}
