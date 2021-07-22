
#ifndef GRAPH
#define GRAPH

#include <vector>
#include <fstream>
#include <stdlib.h>
#include "communicator.hpp"
#include "vertex.hpp"
#include "log.h"

#include <assert.h>
#include <fcntl.h>
#include <sys/stat.h>
inline long file_size(const char *filename) {
  struct stat st;
  assert(stat(filename, &st)==0);
  return st.st_size;
}

template<typename KeyType, typename ValueType>
struct EdgeUnit
{
	KeyType src;
	KeyType dst;
};

template<class KeyType, class ValueType>
class Graph
{
private:
	Communicator *comm;
	// sorted vertex
	std::vector<Vertex<KeyType, ValueType> > low_degree_v;
	std::vector<Vertex<KeyType, ValueType> > high_degree_v;
public:
	Graph(Communicator *comm, char const *argv): comm(comm)
	{
		load(argv);
	}
	~Graph() {};

	void load(char const *argv);
	
};

template<class KeyType, class ValueType>
void Graph<KeyType, ValueType>::load(char const *file_path)
{
	const int size = comm->get_size();
	const int rank = comm->get_rank();
	
	std::ifstream fin(file_path);
	if(!fin.is_open())
	{
		log("Error: Failed to open file\n");
		exit(0);
	}
	long total_bytes = file_size(file_path);

	int edges = total_bytes / sizeof(EdgeUnit<KeyType, ValueType>);
	EdgeUnit<KeyType, ValueType> read_buffer[edges];
	fin.read((char *)read_buffer, total_bytes);

	KeyType src, dst;
	//ValueType value;

	double time_start, time_end;
	time_start = MPI_Wtime();
	for (int i=0; i<edges; i++)
	{
		src = read_buffer[i].src;
		dst = read_buffer[i].dst;

		Vertex<KeyType, ValueType> *v = new Vertex<KeyType, ValueType>(dst);

		if(rank == dst % size)
		{
			auto it = lower_bound(low_degree_v.begin(), low_degree_v.end(), *v);
			if(it == low_degree_v.end() || it->get_id() != dst)
			{
				low_degree_v.insert(it, *v);
				v->add_nbr(src);
			}
			else
				it->add_nbr(src);
		}

	}
	time_end = MPI_Wtime();
	log("time: %lf\n", time_end - time_start);
	fin.close();

	log("Vertices: %d\n", low_degree_v.size());
}

/*
template<class KeyType, class ValueType>
void Graph<KeyType, ValueType>::load(char const *file_path)
{
	const int size = comm->get_size();
	const int rank = comm->get_rank();
	
	std::ifstream infile(file_path);
	if(!infile.is_open())
	{
		log("Error: Failed to open file\n");
		exit(0);
	}

	KeyType src, dst;
	//ValueType value;
	while(!infile.eof())
	{
		infile >> src >> dst;
		Vertex<KeyType, ValueType> *v = new Vertex<KeyType, ValueType>(dst);

		if(rank == dst % size)
		{
			auto it = lower_bound(low_degree_v.begin(), low_degree_v.end(), *v);
			if(it == low_degree_v.end() || it->get_id() != dst)
			{
				low_degree_v.insert(it, *v);
				v->add_nbr(src);
			}
			else
				it->add_nbr(src);
		}

	}
	infile.close();

	log("Vertices: %d\n", low_degree_v.size());
}
*/
#endif