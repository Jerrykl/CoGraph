
#ifndef GRAPH
#define GRAPH

#include <vector>
#include <fstream>
#include <stdlib.h>
#include "communicator.hpp"
#include "vertex.hpp"
#include "message.hpp"
#include "controller.hpp"
#include "bitmap.hpp"
#include "log.h"
#include "sender.hpp"
#include <unordered_map>
#include <set>

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
	typedef Vertex<KeyType, ValueType> VertexType;
	typedef std::unordered_map<KeyType, VertexType> VertexContainer;
	typedef MessageBuffer<KeyType, ValueType> MesgBuf;
	typedef Controller<KeyType, ValueType> ControllerType;

	ControllerType *controller;
	Communicator *comm;
	MesgBuf *mesg_buf;
	size_t threshold;
	ValueType default_value;

	int size;
	int rank;
	

	inline void insert_vertex(VertexContainer &vertex_container, KeyType id, ValueType value, KeyType nbr, bool is_in);
	inline void insert_mirror(KeyType &id, int rank);

	void init_map();
	void init_nbr_ptr(VertexType &v);

public:
	// vertex unordered map
	VertexContainer low_degree_master;
	VertexContainer high_degree_master;

	VertexContainer low_degree_mirror;
	VertexContainer high_degree_mirror;

	unsigned long num_edges;

	/*
		mirror: id -> mirror rank set
		master: id -> master rank
	*/
	std::unordered_map<KeyType, std::set<int> > mirror;
	//std::unordered_map<KeyType, int> master;

	// active sets
	BitMap low_active_master;
	BitMap low_active_mirror;
	BitMap high_active_master;
	BitMap high_active_mirror;

	/*
		id map
		global to local
		local to global
	*/
	std::unordered_map<KeyType, KeyType> gtol_low_master;
	std::unordered_map<KeyType, KeyType> gtol_low_mirror;
	std::unordered_map<KeyType, KeyType> gtol_high_master;
	std::unordered_map<KeyType, KeyType> gtol_high_mirror;

	std::unordered_map<KeyType, KeyType> ltog_low_master;
	std::unordered_map<KeyType, KeyType> ltog_low_mirror;
	std::unordered_map<KeyType, KeyType> ltog_high_master;
	std::unordered_map<KeyType, KeyType> ltog_high_mirror;

	Graph(ControllerType *controller, char const *file_path, size_t threshold, ValueType default_value):controller(controller), threshold(threshold), default_value(default_value)
	{
		comm = controller->comm;
		size = comm->get_size();
		rank = comm->get_rank();
		mesg_buf = controller->mesg_buf;
		load(file_path);
	}
	~Graph() {}

	void load(char const *file_path);

	inline int hash(KeyType id) {return id % size;}
	inline VertexType &find_vertex(KeyType id);
	inline bool insert_active_master(KeyType id);
	inline bool insert_active_mirror(KeyType id);

	void transform_vertices(bool (*init_fun)(VertexType &v));

};

template<class KeyType, class ValueType>
void Graph<KeyType, ValueType>::load(char const *file_path)
{
	
	
	std::ifstream fin(file_path);
	if(!fin.is_open())
	{
		log("Error: Failed to open file\n");
		exit(0);
	}

	double time_start, time_end;
	time_start = MPI_Wtime();

	long total_bytes = file_size(file_path);

	num_edges = total_bytes / sizeof(EdgeUnit<KeyType, ValueType>);
	EdgeUnit<KeyType, ValueType> *read_buffer = new EdgeUnit<KeyType, ValueType>[num_edges];
	fin.read((char *)read_buffer, total_bytes);

	KeyType src, dst;

	// assign low degree vertices
	for (unsigned long i=0; i<num_edges; i++)
	{
		src = read_buffer[i].src;
		dst = read_buffer[i].dst;
		int dst_rank = hash(dst);
		if(rank == dst_rank)
		{
			// add dst: src -> dst
			insert_vertex(low_degree_master, dst, default_value, src, true);
			// auto ldma_it = low_degree_master.find(dst);
			// if(low_degree_master.end() == ldma_it)
			// {
			// 	VertexType v(dst);
			// 	v.add_in_nbr(src);
			// 	low_degree_master.insert(std::make_pair(dst, v));
			// }
			// else
			// 	(ldma_it->second).add_in_nbr(src);
			
			// add out nbrs: both src and dst are on current rank, add src -> dst
			if(hash(src) == rank)
			{
				insert_vertex(low_degree_master, src, default_value, dst, false);
				// ldma_it = low_degree_master.find(src);
				// if(low_degree_master.end() == ldma_it)
				// {
				// 	VertexType v(src);
				// 	v.add_out_nbr(dst);
				// 	low_degree_master.insert(std::make_pair(src, v));
				// }
				// else
				// 	(ldma_it->second).add_out_nbr(dst);
			}
			// else
			// {
			// 	insert_vertex(low_degree_mirror, src, 0, dst, false);
			// 	// auto ldmi_it = low_degree_mirror.find(src);
			// 	// if(low_degree_mirror.end() == ldmi_it)
			// 	// {
			// 	// 	VertexType v(src);
			// 	// 	v.add_out_nbr(dst);
			// 	// 	low_degree_mirror.insert(std::make_pair(src, v));
			// 	// }
			// 	// else
			// 	// 	(ldmi_it->second).add_out_nbr(dst);
			// }

		}
		else
			if(hash(src) == rank)
			{
				// add mirror for src: dst not at current rank & src at current rank, so src is mirror at dst_rank
				insert_vertex(low_degree_master, src, default_value, src, true);
				// insert_mirror(src, dst_rank);
				// auto mirror_it = mirror.find(src);
				// if(mirror.end() == mirror_it)
				// {
				// 	std::set<int> set;
				// 	set.insert(dst_rank);
				// 	mirror.insert(std::make_pair(src, set));
				// }
				// else
				// 	(mirror_it->second).insert(dst_rank);
			}
	}
	
	log("assign low degree time:%lf\n", MPI_Wtime() - time_start);

	// reassign high degree vertices
	// int mesg_num_send[size];
	// int mesg_num_recv[size];
	// memset(mesg_num_send, 0, sizeof(int) * size);
	Sender<typename MesgBuf::EdgeMesg> *sender = new Sender<typename MesgBuf::EdgeMesg>(comm, EDGE);
	for(auto ldma_it = low_degree_master.begin(); ldma_it != low_degree_master.end();)
	{
		auto &in_nbrs = (ldma_it->second).get_in_nbr();
		// if(ldma_it->first == 5750)
		// 	log("rank:%d, id: %d, in_nbrs:%d\n", rank, ldma_it->first, in_nbrs.size());
		if(in_nbrs.size() > threshold)
		{
			KeyType dst = ldma_it->first;
			ValueType dst_value = (ldma_it->second).get_value();

			// high_active.insert(dst);
			insert_vertex(high_degree_master, dst, default_value, dst, true);// in case no src at current rank
			
			auto mirror_it = mirror.find(dst);
			if(mirror.end() == mirror_it)
			{
				std::set<int> set;
				auto pair = mirror.insert(make_pair(dst, set));
				mirror_it = pair.first;
			}
			for (int i = 0; i < size; ++i)
			{
				if(i == rank)
					continue;
				typename MesgBuf::EdgeMesg *bucket = sender->get_bucket(i);
				bucket->src = dst;
				bucket->dst = dst;
				bucket->dst_value = dst_value;
				// typename MesgBuf::EdgeMesg em = {dst, dst, dst_value};
				// comm->send(i, EDGE, (void *)&em, sizeof(em));
				// mesg_num_send[i]++;
			}
			
			for(auto & src : in_nbrs)
			{
				int src_rank = hash(src);
				if(src_rank == rank)
				{
					// add vertex to high degree master
					insert_vertex(high_degree_master, dst, default_value, src, true);
					// auto &out_nbrs = (ldma_it->second).get_out_nbr();
					// if(dst == 0)
					// 	printf("Note:0 have %d\n", out_nbrs.size());

					(high_degree_master.find(dst)->second).get_out_nbr() = ((ldma_it->second).get_out_nbr());
					// (high_degree_master.find(dst)->second).get_out_nbr() = std::move((ldma_it->second).get_out_nbr());
					// out_nbrs = (high_degree_master.find(dst)->second).get_out_nbr();
					// if(dst == 0)
					// 	printf("Note:0 have %d\n", (high_degree_master.find(dst)->second).get_out_nbr().size());

					// auto it = high_degree_master.find(dst);
					// if(it == high_degree_master.end())
					// {
					// 	VertexType v(dst);
					// 	v.add_in_nbr(src);
					// 	high_degree_master.insert(std::make_pair(dst, v));
					// }
					// else
					// 	(it->second).add_in_nbr(src);
				}
				else
				{
					// src at other rank, add mirror
					(mirror_it->second).insert(src_rank);
					//ValueType src_value = ((low_degree_mirror.find(src))->second).get_value();
					//double temp = MPI_Wtime();
					typename MesgBuf::EdgeMesg *bucket = sender->get_bucket(src_rank);
					bucket->src = src;
					bucket->dst = dst;
					bucket->dst_value = dst_value;
					// typename MesgBuf::EdgeMesg em = {src, dst, dst_value};
					// comm->send(src_rank, EDGE, (void *)&em, sizeof(em));
					// mesg_num_send[src_rank]++;

					//log("Send edge time %lf\n", MPI_Wtime() - temp);
					// double temp = MPI_Wtime();
					// typename MesgBuf::SyncMesg sync_mesg = {src, dst_value};
					// comm->send(src_rank, SYNC_MASTER, (void *)&sync_mesg, sizeof(sync_mesg));
					// log("Test send time %lf\n", MPI_Wtime() - temp);
				}
			}

			// delete the original vertex
			ldma_it = low_degree_master.erase(ldma_it);
		}
		else
			++ldma_it;

	}
	sender->flush();
	delete sender;
	MPI_Barrier(comm->mpi_comm);
	while(comm->is_receiving());
	// MPI_Alltoall(mesg_num_send, 1, MPI_INT, mesg_num_recv, 1, MPI_INT, comm->mpi_comm);
	// int sum = 0;
	// for (int i = 0; i < size; i++)
	// 	sum += mesg_num_recv[i];
	// while(mesg_buf->buf_size != sum);

	log("reassign high degree time:%lf\n", MPI_Wtime() - time_start);

	/*
		receive high degree mirror from other ranks
	*/
	// log("Rank:%d, Size:%d\n", comm->get_rank(), (mesg_buf->get_edge_buf()).size());
	for(auto block_ptr : mesg_buf->get_edge_buf())
	{
		int buf_size = block_ptr->id;
		for (int i = 1; i < buf_size + 1; i++)
		{
			auto edge_ptr = block_ptr + i;
			insert_vertex(high_degree_mirror, edge_ptr->dst, edge_ptr->dst_value, edge_ptr->src, true);
			if(edge_ptr->dst != edge_ptr->src)
			{
				if(high_degree_master.end() != high_degree_master.find(edge_ptr->src))
					insert_vertex(high_degree_master, edge_ptr->src, default_value, edge_ptr->dst, false);
				else
					if(low_degree_master.end() != low_degree_master.find(edge_ptr->src))
						insert_vertex(low_degree_master, edge_ptr->src, default_value, edge_ptr->dst, false);
					else
					{
						log("Failed insert mirror, rank:%d dst: %d, src:%d, dst_value:%d\n", rank, edge_ptr->dst, edge_ptr->src, edge_ptr->dst_value);
						exit(0);
					}
			}
		}
		free(block_ptr);
		// free(edge_ptr);
		// high_active.insert(edge_ptr->dst); // insert into active set

	}
	// log("Rank:%d, Size:%d\n", comm->get_rank(), (mesg_buf->get_edge_buf()).size());
	(mesg_buf->get_edge_buf()).clear();

	log("receive high degree time:%lf\n", MPI_Wtime() - time_start);

	/*
		initialize low degree mirror
		add out nbrs
	*/
	for(auto & pair : low_degree_master)
	{
		KeyType	dst = pair.first;
		// low_active_master.insert(dst); // insert into active set
		auto &in_nbrs = pair.second.get_in_nbr();
		for(auto & src : in_nbrs)
		{
			if(hash(src) != rank)
			{
				if(high_degree_mirror.end() == high_degree_mirror.find(src))
				{
					// low_active_mirror.insert(src); // insert into active set
					insert_vertex(low_degree_mirror, src, default_value, dst, false);
					// auto ldmi_it = low_degree_mirror.find(src);
					// if(low_degree_mirror.end() != ldmi_it)
					// {
					// 	VertexType v(src);
					// 	v.add_out_nbr(dst);
					// 	low_degree_mirror.insert(std::make_pair(src, v));
					// }
					// else
					// 	(ldmi_it->second).add_out_nbr(dst);
				}
				else
				{
					insert_vertex(high_degree_mirror, src, default_value, dst ,false);
				}
			}
		}
	}
	log("initialize low degree mirror time:%lf\n", MPI_Wtime() - time_start);

	for (unsigned long i=0; i<num_edges; i++)
	{
		src = read_buffer[i].src;
		dst = read_buffer[i].dst;
		int dst_rank = hash(dst);
		if((rank != dst_rank) && (hash(src) == rank) && (high_degree_mirror.end() == high_degree_mirror.find(dst)))
			insert_mirror(src, dst_rank);
	}
	delete []read_buffer;
	
	init_map();


	time_end = MPI_Wtime();
	log("loading time: %lf\n", time_end - time_start);
	MPI_Barrier(comm->mpi_comm);

	fin.close();

	log("rank: %d, low master Vertices: %d\n", rank, low_degree_master.size());
	log("rank: %d, high master Vertices: %d\n", rank, high_degree_master.size());
	log("rank: %d, low mirror Vertices: %d\n", rank, low_degree_mirror.size());
	log("rank: %d, high mirror Vertices: %d\n", rank, high_degree_mirror.size());

	unsigned long local_v[4] = {low_degree_master.size(), high_degree_master.size(), low_degree_mirror.size(), high_degree_mirror.size()};
	unsigned long global_v[4];
	MPI_Reduce(&local_v, &global_v, 4, MPI_UNSIGNED_LONG, MPI_SUM, 0, comm->mpi_comm);
	if(0 == rank) {
		printf("low master:%ld, high master:%ld, low mirror:%ld, high mirror:%ld, Total mirror:%ld\n", global_v[0], global_v[1],
		 global_v[2], global_v[3], global_v[2] + global_v[3]);
		printf("Loading time: %lf (s)\n", MPI_Wtime()-time_start);
	}
}

template<class KeyType, class ValueType>
inline void Graph<KeyType, ValueType>::insert_vertex(VertexContainer &vertex_container, KeyType id, ValueType value, KeyType nbr, bool is_in)
{
	auto vc_it = vertex_container.find(id);
	
	if(vertex_container.end() == vc_it)
	{
		VertexType v = VertexType(id, value);
		
		// not broadcast init
		if(id != nbr)
		{
			if(is_in)
				v.add_in_nbr(nbr);
			else
				v.add_out_nbr(nbr);
		}

		vertex_container.insert(std::make_pair(id, v));
	}
	else
	{
		if(id != nbr)
		{
			if(is_in)
				(vc_it->second).add_in_nbr(nbr);
			else
				(vc_it->second).add_out_nbr(nbr);
		}
	}
}

template<class KeyType, class ValueType>
inline void Graph<KeyType, ValueType>::insert_mirror(KeyType &id, int rank)
{

	auto mirror_it = mirror.find(id);
	if(mirror.end() == mirror_it)
	{
		std::set<int> set;
		set.insert(rank);
		mirror.insert(std::make_pair(id, set));
	}
	else
		(mirror_it->second).insert(rank);

}

template<class KeyType, class ValueType>
inline Vertex<KeyType, ValueType> &Graph<KeyType, ValueType>::find_vertex(KeyType id)
{
	typename VertexContainer::iterator vc_it;
	if(hash(id) == rank)
	{
		if(low_degree_master.end() != (vc_it = low_degree_master.find(id)))
			return vc_it->second;
		if(high_degree_master.end() != (vc_it = high_degree_master.find(id)))
			return vc_it->second;
	}
	else
	{
		if(low_degree_mirror.end() != (vc_it = low_degree_mirror.find(id)))
			return vc_it->second;
		if(high_degree_mirror.end() != (vc_it = high_degree_mirror.find(id)))
			return vc_it->second;
	}
	log("Rank:%d, Vertex %d not found\n", comm->get_rank(), id);
	exit(0);
}

template<class KeyType, class ValueType>
inline bool Graph<KeyType, ValueType>::insert_active_master(KeyType id)
{
	if(high_degree_master.end() != high_degree_master.find(id))
	{
		KeyType lid = gtol_high_master[id];
		return high_active_master.set_bit(lid);
	}
	else
	{
		KeyType lid = gtol_low_master[id];
		return low_active_master.set_bit(lid);
	}
}

template<class KeyType, class ValueType>
inline bool Graph<KeyType, ValueType>::insert_active_mirror(KeyType id)
{
	if(high_degree_mirror.end() != high_degree_mirror.find(id))
	{
		KeyType lid = gtol_high_mirror[id];
		return high_active_mirror.set_bit(lid);
	}
	else
	{
		KeyType lid = gtol_low_mirror[id];
		return low_active_mirror.set_bit(lid);
	}
}

template<class KeyType, class ValueType>
void Graph<KeyType, ValueType>::init_map()
{
	#pragma omp parallel sections num_threads(4)
	{
		#pragma omp section
		{
			KeyType low_master_index = 0;
			low_active_master.init(low_degree_master.size());
			for(auto &pair : low_degree_master)
			{
				KeyType id = pair.first;
				gtol_low_master[id] = low_master_index;
				ltog_low_master[low_master_index] = id;
				// low_active_master.set_bit(low_master_index);
				low_master_index++;
			}
		}
		#pragma omp section
		{
			KeyType low_mirror_index = 0;
			low_active_mirror.init(low_degree_mirror.size());
			for(auto &pair : low_degree_mirror)
			{
				KeyType id = pair.first;
				gtol_low_mirror[id] = low_mirror_index;
				ltog_low_mirror[low_mirror_index] = id;
				// low_active_mirror.set_bit(low_mirror_index);
				low_mirror_index++;
			}
		}
		#pragma omp section
		{
			KeyType high_master_index = 0;
			high_active_master.init(high_degree_master.size());
			for(auto &pair : high_degree_master)
			{
				KeyType id = pair.first;
				gtol_high_master[id] = high_master_index;
				ltog_high_master[high_master_index] = id;
				// high_active_master.set_bit(high_master_index);
				high_master_index++;
			}
		}
		#pragma omp section
		{
			KeyType high_mirror_index = 0;
			high_active_mirror.init(high_degree_mirror.size());
			for(auto &pair : high_degree_mirror)
			{
				KeyType id = pair.first;
				gtol_high_mirror[id] = high_mirror_index;
				ltog_high_mirror[high_mirror_index] = id;
				// high_active_mirror.set_bit(high_mirror_index);
				high_mirror_index++;
			}
		}
	}
}

template<class KeyType, class ValueType>
void Graph<KeyType, ValueType>::transform_vertices(bool (*init_fun)(VertexType &v))
{
	#pragma omp parallel sections num_threads(4)
	{
		#pragma omp section
		{
			for(auto &pair : low_degree_master)
			{
				VertexType &v = pair.second;
				init_nbr_ptr(v);
				if(init_fun(v))
					low_active_master.set_bit(gtol_low_master[v.get_id()]);
			}
		}
		#pragma omp section
		{
			for(auto &pair : low_degree_mirror)
			{
				VertexType &v = pair.second;
				init_nbr_ptr(v);
				if(init_fun(v))
					low_active_mirror.set_bit(gtol_low_mirror[v.get_id()]);
			}
		}
		#pragma omp section
		{
			for(auto &pair : high_degree_master)
			{
				VertexType &v = pair.second;
				init_nbr_ptr(v);
				if(init_fun(v))
					high_active_master.set_bit(gtol_high_master[v.get_id()]);
			}
		}
		#pragma omp section
		{
			for(auto &pair : high_degree_mirror)
			{
				VertexType &v = pair.second;
				init_nbr_ptr(v);
				if(init_fun(v))
					high_active_mirror.set_bit(gtol_high_mirror[v.get_id()]);
			}
		}
	}
}

template<class KeyType, class ValueType>
void Graph<KeyType, ValueType>::init_nbr_ptr(VertexType &v)
{
	for(auto in_nbr : v.get_in_nbr())
		v.in_nbrs_v.push_back(&find_vertex(in_nbr));
	for(auto out_nbr : v.get_out_nbr())
		v.out_nbrs_v.push_back(&find_vertex(out_nbr));
	v.clear_nbr_vec(); // free the nbr vector which would be unused
}

#endif