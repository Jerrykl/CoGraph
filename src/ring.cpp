
#include "controller.hpp"
#include "graph.hpp"
#include "engine.hpp"
#include "vertexprogram.hpp"
#include <algorithm>

struct VecValue {
	std::vector<int> value;
};

VecValue operator-(const VecValue& v1, const VecValue& v2) {
	return VecValue{ value: {v1.value[0] - v2.value[0]} };
}

VecValue operator+(const VecValue& v1, const VecValue& v2) {
	return VecValue{ value: {v1.value[0] + v2.value[0]} };
}

bool operator<(const VecValue& v1, const VecValue& v2) {
	return v1.value[0] < v2.value[0];
}

bool operator!=(const VecValue& v1, const VecValue& v2) {
	return v1.value[0] != v2.value[0];
}

VecValue operator+(const VecValue& v1, int n) {
	return VecValue{ value: {v1.value[0] + n} };
}

bool operator!(const VecValue& v) {
	return v.value[0] == 0;
}

template<class KeyType, class ValueType>
class Ring: public VertexProgram<KeyType, ValueType>
{
private:
	const ValueType delta = VecValue{{0}};
	//ValueType change;
	typedef Vertex<KeyType, ValueType> VertexType;

public:
	static KeyType source;
	Ring() {this->iterations = 64; this->acc_init = VecValue { value: {1 << 30}};}
	~Ring() {}
	
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
		v.set_value({std::min(old_value, total + 1)});
		if(v.get_id() == source)
			v.set_value(VecValue{{0}});
		v.change = {v.get_value() - old_value};
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
			v.set_value(VecValue {{1 << 30}});
			v.is_active = true;
			return true; // active
		}
		else
		{
			v.set_value(VecValue {{1 << 30}});
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
KeyType Ring<KeyType, ValueType>::source = 0;

int main(int argc, char const *argv[])
{
	Controller<unsigned int, VecValue> controller(argc, argv);

	char const *file_path = argv[1];
	int threshold = 1000;
	if(argc >= 3)
		threshold = (size_t)atoi(argv[2]);
	if(argc >= 4)
		Ring<unsigned int, VecValue>::source = atoi(argv[3]);

	log("threshold:%d\n", threshold);

	Ring<unsigned int, VecValue> ring;
	
	Graph<unsigned int, VecValue> graph(&controller, file_path, threshold, VecValue{{1}});
	graph.transform_vertices(Ring<unsigned int, VecValue>::init_vertex);
	
	Engine<unsigned int, VecValue> engine(&controller, &graph, &ring);
	engine.run();
	return 0;
}