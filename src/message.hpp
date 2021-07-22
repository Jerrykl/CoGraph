
#ifndef MESSAGE
#define MESSAGE

#include <vector>
#include <unordered_map>
#include <omp.h>
#include <stdlib.h>
#include "log.h"
#include "vertex.hpp"
#include "vertexprogram.hpp"

template<class KeyType, class ValueType>
class Graph;

typedef enum
{
	EDGE = 0,
	SYNC_MASTER_LOW = 1,
	SYNC_MASTER_HIGH = 2,
	SYNC_MIRROR_LOW = 3,
	SYNC_MIRROR_HIGH = 4,
	ACT = 5
} TAG;

// template<class KeyType, class ValueType>
// extern inline ValueType op(ValueType x, ValueType y);

template<class KeyType, class ValueType>
class MessageBuffer
{
public:
	struct EdgeMesg
	{
		//char mark;
		KeyType id;
		KeyType src;
		KeyType dst;
		//ValueType src_value;
		ValueType dst_value;
		//ValueType edge_value;
	};
	struct SyncMesg
	{
		//char mark;
		KeyType id;
		ValueType value;
	};
	struct ActMesg
	{
		//char mark;
		KeyType id;
	};
	typedef	std::vector<EdgeMesg*> EDGE_BUF;
	typedef Vertex<KeyType, ValueType> VertexType;
	typedef VertexProgram<KeyType, ValueType> VertexProgType;

	// storing value from mirrors, indexed by local id
	ValueType *sync_buf_low;
	ValueType *sync_buf_high; 
	void init(Graph<KeyType, ValueType> *graph, VertexProgType *v_prog)
	{
		this->graph = graph;
		this->v_prog = v_prog;
		sync_buf_low = (ValueType *)malloc(sizeof(ValueType) * graph->low_degree_master.size());
		sync_buf_high = (ValueType *)malloc(sizeof(ValueType) * graph->high_degree_master.size());
	}
	
	MessageBuffer(int rank):rank(rank) {}
	~MessageBuffer() {free(sync_buf_low); free(sync_buf_high);}

	inline void process_mesg(int tag, void *buf);
	
	inline EDGE_BUF & get_edge_buf() {return edge_buf;}


	inline ValueType *get_sync_buf_low() {return sync_buf_low;}
	inline ValueType *get_sync_buf_high() {return sync_buf_high;}

	// volatile int buf_size;

	
private:
	int rank;
	Graph<KeyType, ValueType> *graph;
	VertexProgType *v_prog;
	
	EDGE_BUF edge_buf;

	std::pair<KeyType, SyncMesg*> sync_local_buf; // mesg generated by local master
};


template<class KeyType, class ValueType>
inline void MessageBuffer<KeyType, ValueType>::process_mesg(int tag, void *buf)
{
	//char* mark = (char*)buf;
	switch(tag)
	{
		case EDGE:
		{
			#pragma omp critical
			edge_buf.push_back((EdgeMesg*)buf);
			// buf_size = edge_buf.size();
			break;
		}
		case SYNC_MASTER_LOW:
		{
			int size = ((SyncMesg *)buf)->id;
			for (int i = 1; i < size + 1; ++i)
			{
				SyncMesg *mesg = (SyncMesg *)buf+i;
				KeyType lid = graph->gtol_low_master[mesg->id];
				ValueType expected, acc;
				do
				{
					expected = sync_buf_low[lid];
					acc = v_prog->op(sync_buf_low[lid], mesg->value);
				}while(!__atomic_compare_exchange(sync_buf_low + lid, &expected, &acc, true, __ATOMIC_RELAXED, __ATOMIC_RELAXED));
				// VertexType &v = (graph->low_degree_master).find(mesg->id);
				// __sync_bool_compare_and_swap(&(v.value), v.value, op<KeyType, ValueType>(v.value, mesg->value));
			}
			free(buf);
			break;
		}
		case SYNC_MASTER_HIGH:
		{
			int size = ((SyncMesg *)buf)->id;
			for (int i = 1; i < size + 1; ++i)
			{
				SyncMesg *mesg = (SyncMesg *)buf+i;
				KeyType lid = graph->gtol_high_master[mesg->id];
				// log("rank: %d, LID: %d, GID: %d\n", rank, lid, mesg->id);
				ValueType expected, acc;
				do
				{
					expected = sync_buf_high[lid];
					acc = v_prog->op(sync_buf_high[lid], mesg->value);
				}while(!__atomic_compare_exchange(sync_buf_high + lid, &expected, &acc, true, __ATOMIC_RELAXED, __ATOMIC_RELAXED));
				// VertexType &v = (graph->high_degree_master).find(mesg->id);
				// __sync_bool_compare_and_swap(&(v.value), v.value, op<KeyType, ValueType>(v.value, mesg->value));
			}
			free(buf);
			break;
		}
		case SYNC_MIRROR_LOW:
		{
			int size = ((SyncMesg *)buf)->id;
			for (int i = 1; i < size + 1; ++i)
			{
				SyncMesg *mesg = (SyncMesg *)buf+i;
				// if(graph->low_degree_mirror.end() == (graph->low_degree_mirror).find(mesg->id))
				// 	log("rank: %d, ID: %d, NO MIRROR\n", rank, mesg->id);
				VertexType &v = ((graph->low_degree_mirror).find(mesg->id))->second;
				ValueType old_value = v.get_value();
				v.set_value(mesg->value);
				v.change = v.get_value() - old_value;
				// log("Note:rank: %d, ID:%d old_value:%d, new_value:%d\n", rank, mesg->id, old_value, v.get_value());
			}
			free(buf);
			break;
		}
		case SYNC_MIRROR_HIGH:
		{
			int size = ((SyncMesg *)buf)->id;
			for (int i = 1; i < size + 1; ++i)
			{
				SyncMesg *mesg = (SyncMesg *)buf+i;
				VertexType &v = ((graph->high_degree_mirror).find(mesg->id))->second;
				ValueType old_value = v.get_value();
				v.set_value(mesg->value);
				v.change = v.get_value() - old_value;
			}
			free(buf);
			break;
		}
		case ACT:
		{
			int size = ((ActMesg *)buf)->id;
			for (int i = 1; i < size + 1; ++i)
			{
				ActMesg *mesg = (ActMesg *)buf+i;
				if(rank == graph->hash(mesg->id))
					graph->insert_active_master(mesg->id);
				else
					graph->insert_active_mirror(mesg->id);
			}
			free(buf);
			break;
		}
		default: log("Wrong tag: %d\n", (int)tag);exit(0);
	}
}


#endif