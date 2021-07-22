

#ifndef VERTEX
#define VERTEX

#include <vector>

template <class KeyType, class ValueType>
class Vertex;

template <class KeyType, class ValueType>
class Vertex
{
private:
	KeyType id;
	ValueType value;
	std::vector<KeyType> *in_nbrs; // id <- nbr
	std::vector<KeyType> *out_nbrs; // id -> nbr


public:
	std::vector<Vertex<KeyType, ValueType> *> in_nbrs_v;
	std::vector<Vertex<KeyType, ValueType> *> out_nbrs_v;
	ValueType change;
	volatile bool is_active;
	Vertex(KeyType id, ValueType value): id(id), value(value)
	{
		in_nbrs = new std::vector<KeyType>;
		out_nbrs = new std::vector<KeyType>;
	}
	~Vertex() {};
	
	inline bool operator<(const Vertex &v)
	{
		return id < v.id;
	}

	inline KeyType &get_id() {return id;}
	inline ValueType &get_value() {return value;}
	inline void set_value(ValueType val) {value = val;}

	inline void add_in_nbr(KeyType src) {in_nbrs->push_back(src);}
	inline std::vector<KeyType>& get_in_nbr() {return *in_nbrs;}

	inline void add_out_nbr(KeyType dst) {out_nbrs->push_back(dst);}
	inline std::vector<KeyType>& get_out_nbr() {return *out_nbrs;}

	inline size_t in_nbrs_size() { return in_nbrs_v.size(); }
	inline size_t out_nbrs_size() { return out_nbrs_v.size(); }

	inline void clear_nbr_vec()
	{
		delete in_nbrs;
		delete out_nbrs;
		in_nbrs = NULL;
		out_nbrs = NULL;
	}
};


#endif