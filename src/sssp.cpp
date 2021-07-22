
#include "controller.hpp"
#include "graph.hpp"
#include "engine.hpp"
#include "vertexprogram.hpp"
#include <algorithm>

template<class KeyType, class ValueType>
class Sssp: public VertexProgram<KeyType, ValueType>
{
private:
	const ValueType delta = 0;
	//ValueType change;
	typedef Vertex<KeyType, ValueType> VertexType;

public:
	static KeyType source;
	Sssp() {this->iterations = 64; this->acc_init = 1 << 30;}
	~Sssp() {}
	
	EDGE_DIRECTION gather_edge() {return IN_EDGES;}
	EDGE_DIRECTION gather_edge(VertexType &v)
	{
		if(v.get_id() == source)
			return NO_EDGES;
		else
			return IN_EDGES;
	}
	
	EDGE_DIRECTION scatter_edge() {return OUT_EDGES;}
	EDGE_DIRECTION scatter_edge(VertexType &v)
	{
		if(delta != v.change)
			return OUT_EDGES;
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
		v.set_value(std::min(old_value, total + 1));
		if(v.get_id() == source)
			v.set_value(0);
		v.change = v.get_value() - old_value;
		// log("Warning: %d: old_value:%d, new_value:%d\n", v.get_id(), old_value, v.get_value());
	}

	bool scatter(VertexType &v, VertexType &nbr)
	{
		if(v.get_value() + 1 < nbr.get_value())
		{
			// log("Warning: %d(%d) activate %d(%d)\n", v.get_id(), v.get_value(), nbr.get_id(), nbr.get_value());
			return true;
		}
		else
			return false;
	}
	
	static bool init_vertex(VertexType &v)
	{
		if(v.get_id() == source)
		{
			v.set_value(1 << 30);
			v.is_active = true;
			return true; // active
		}
		else
		{
			v.set_value(1 << 30);
			v.is_active = false;
			return false; // inactive
		}
	}

};

template<class KeyType, class ValueType>
ValueType op(ValueType x, ValueType y)
{
	return std::min(x, y);
}

template<class KeyType, class ValueType>
KeyType Sssp<KeyType, ValueType>::source = 0;

int main(int argc, char *argv[])
{
	int opt;
	char *file_path = NULL;
	size_t threshold = 1000;

    while ((opt = getopt(argc, argv, "g:t:s:")) != -1) {
        switch (opt) {
        case 'g':
            file_path = optarg;
            break;
        case 't':
            threshold = (size_t)atoi(optarg);
            break;
        case 's':
        	Sssp<unsigned int, unsigned int>::source = atoi(optarg);
        	break;
        default:
            fprintf(stderr, "Usage: %s [-g graph] [-t threshold] [-s source]\n", argv[0]);
            exit(EXIT_FAILURE);
        }
    }

	if (file_path == NULL) {
		fprintf(stderr, "Usage: %s [-g graph] [-t threshold] [-s source]\n", argv[0]);
		exit(EXIT_FAILURE);
	}

	Controller<unsigned int, unsigned int> controller;

	Sssp<unsigned int, unsigned int> sssp;
	
	Graph<unsigned int, unsigned int> graph(&controller, file_path, threshold, 1);
	graph.transform_vertices(Sssp<unsigned int, unsigned int>::init_vertex);
	
	Engine<unsigned int, unsigned int> engine(&controller, &graph, &sssp);
	engine.run();
	return 0;
}