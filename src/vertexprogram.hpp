
#ifndef VERTEXPROGRAM
#define VERTEXPROGRAM

typedef enum
{
	NO_EDGES = 0, // b'00
	IN_EDGES = 1, // b'01
	OUT_EDGES = 2,// b'10
	ALL_EDGES = 3 // b'11
} EDGE_DIRECTION;


template<class KeyType, class ValueType>
class VertexProgram
{
private:
	typedef Vertex<KeyType, ValueType> VertexType;
public:
	int iterations;
	ValueType acc_init;
	VertexProgram() {}
	~VertexProgram() {}

	virtual EDGE_DIRECTION gather_edge() = 0;
	virtual EDGE_DIRECTION gather_edge(VertexType &v) = 0;
	virtual EDGE_DIRECTION scatter_edge() = 0;
	virtual EDGE_DIRECTION scatter_edge(VertexType &v) = 0;
	virtual ValueType gather(VertexType &v, VertexType &nbr) = 0;
	virtual ValueType op(ValueType x, ValueType y) = 0;
	virtual void apply(VertexType &v, ValueType total) = 0;
	virtual bool scatter(VertexType &v, VertexType &nbr) = 0;
};

#endif