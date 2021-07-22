
#ifndef ENGINE
#define ENGINE

#include <mpi.h>
#include "vertexprogram.hpp"
#include "sender.hpp"

// #define SKIP_SCATTER

template<class KeyType, class ValueType>
class Engine
{
private:
	typedef Vertex<KeyType, ValueType> VertexType;
	typedef Controller<KeyType, ValueType> ControllerType;
	typedef MessageBuffer<KeyType, ValueType> MesgBuf;
	typedef Graph<KeyType, ValueType> GraphType;
	typedef VertexProgram<KeyType, ValueType> VertexProgType;

	int max_iterations;
	ValueType acc_init;
	ControllerType *controller;
	Communicator *comm;
	MesgBuf *mesg_buf;
	GraphType *graph;
	VertexProgType *v_prog;


	std::pair<KeyType, KeyType*> low_master_active_array;
	std::pair<KeyType, KeyType*> low_mirror_active_array;
	std::pair<KeyType, KeyType*> high_master_active_array;
	std::pair<KeyType, KeyType*> high_mirror_active_array;

	

	int rank;
	int mpi_size;

	double gather_comm_time = 0;
	double apply_comm_time = 0;
	double scatter_comm_time = 0;

	KeyType *num_in_edges = new KeyType[COMP_THREADS];
	KeyType *num_out_edges = new KeyType[COMP_THREADS];

	typedef enum
	{
		LOW_MASTER = 0,
		LOW_MIRROR = 1,
		HIGH_MASTER = 2,
		HIGH_MIRROR = 3
	} VTYPE;

	inline void bitmap_to_array(BitMap &active_set, std::pair<KeyType, KeyType*> &active_array);
	inline void bitmap_to_array_all();

	inline void execute_gather();
	inline void engine_gather_master(std::pair<KeyType, KeyType*> active_array, VTYPE type);
	inline void engine_gather_mirror(std::pair<KeyType, KeyType*> active_array, VTYPE type);

	inline void execute_apply();
	inline void engine_apply(std::pair<KeyType, KeyType*> active_array, VTYPE type, Sender<typename MesgBuf::SyncMesg> *sender[]);

	inline void execute_scatter();
	inline void engine_scatter(std::pair<KeyType, KeyType*> active_array, VTYPE type, Sender<typename MesgBuf::ActMesg> *sender[]);


	inline void gather_apply();

	inline void irecv();

public:
	Engine(ControllerType *controller, GraphType *graph, VertexProgType *v_prog):controller(controller), graph(graph), v_prog(v_prog)
	{
		comm = controller->comm;
		rank = comm->get_rank();
		mpi_size = comm->get_size();
		mesg_buf = controller->mesg_buf;
		max_iterations = v_prog->iterations;
		acc_init = v_prog->acc_init;

		mesg_buf->init(graph, v_prog);

		low_master_active_array.second =  new KeyType[graph->low_degree_master.size()];
		low_mirror_active_array.second =  new KeyType[graph->low_degree_mirror.size()];
		high_master_active_array.second = new KeyType[graph->high_degree_master.size()];
		high_mirror_active_array.second = new KeyType[graph->high_degree_mirror.size()];
		bitmap_to_array_all();
	}
	~Engine()
	{
		delete num_in_edges;
		delete num_out_edges;

		delete low_master_active_array.second;
		delete low_mirror_active_array.second;
		delete high_master_active_array.second;
		delete high_mirror_active_array.second;
	}

	void run();
};

template<class KeyType, class ValueType>
void Engine<KeyType, ValueType>::run()
{
	int i = 0;
	bool conversed = false;
	KeyType local_active;
	KeyType global_active;
	MPI_Barrier(comm->mpi_comm);
	double start = MPI_Wtime();
	double s = MPI_Wtime();
	while(!conversed && i < max_iterations)
	{
		start = MPI_Wtime();
		execute_gather();
		MPI_Barrier(comm->mpi_comm);
		log("gather time: %lf\n", MPI_Wtime() - start);
		start = MPI_Wtime();

		execute_apply();
		MPI_Barrier(comm->mpi_comm);
		log("apply time: %lf\n", MPI_Wtime() - start);
		start = MPI_Wtime();

		execute_scatter();
		log("scatter time: %lf\n", MPI_Wtime() - start);
		
		MPI_Barrier(comm->mpi_comm);

		local_active = (graph->low_active_master).size() + (graph->high_active_master).size();
		MPI_Allreduce(&local_active, &global_active, 1, MPI_INT, MPI_SUM, comm->mpi_comm);
		log("Rank: %d, local active vertices: %d, LOW: %d, HIGH:%d\n", rank, local_active, (graph->low_active_master).size(), (graph->high_active_master).size());
		if(0 == rank)
			printf("Iteration: %d, global_active vertices: %d\n", i, global_active);
		if(!global_active)
			conversed = true;
		i++;
	}
	printf("Rank: %d, GAS (G: %lf, A: %lf, S: %lf, Total: %lf)\n", rank, gather_comm_time, apply_comm_time, scatter_comm_time, gather_comm_time+apply_comm_time+scatter_comm_time);
	MPI_Barrier(comm->mpi_comm);
	if (rank == 0) {
		printf("Total computing time: %lf (s)\n", MPI_Wtime() - s);	
	}

}

template<class KeyType, class ValueType>
inline void Engine<KeyType, ValueType>::gather_apply()
{

	engine_gather_master(low_master_active_array, LOW_MASTER);
	MPI_Barrier(comm->mpi_comm);
	engine_gather_mirror(low_mirror_active_array, LOW_MIRROR);
	MPI_Barrier(comm->mpi_comm);
	while(comm->is_receiving());
	engine_apply(low_master_active_array, LOW_MASTER);
	MPI_Barrier(comm->mpi_comm);
	while(comm->is_receiving());
	
	engine_gather_master(high_master_active_array, HIGH_MASTER);
	MPI_Barrier(comm->mpi_comm);
	engine_gather_mirror(high_mirror_active_array, HIGH_MIRROR);
	MPI_Barrier(comm->mpi_comm);
	while(comm->is_receiving());
	engine_apply(high_master_active_array, HIGH_MASTER);
	MPI_Barrier(comm->mpi_comm);
	while(comm->is_receiving());

	double clear = MPI_Wtime();
	graph->low_active_master.clear();
	graph->low_active_mirror.clear();
	graph->high_active_master.clear();
	graph->high_active_mirror.clear();
	log("clear time of apply: %lf\n", MPI_Wtime() - clear);

	bitmap_to_array_all();
}

template<class KeyType, class ValueType>
inline void Engine<KeyType, ValueType>::execute_gather()
{
	memset(num_in_edges, 0, sizeof(KeyType) * COMP_THREADS);
	memset(num_out_edges, 0, sizeof(KeyType) * COMP_THREADS);

	
	
	engine_gather_master(low_master_active_array, LOW_MASTER);
	engine_gather_master(high_master_active_array, HIGH_MASTER);
	MPI_Barrier(comm->mpi_comm);
	engine_gather_mirror(low_mirror_active_array, LOW_MIRROR);
	engine_gather_mirror(high_mirror_active_array, HIGH_MIRROR);

	// double clear = MPI_Wtime();
	graph->low_active_master.clear();
	graph->low_active_mirror.clear();
	graph->high_active_master.clear();
	graph->high_active_mirror.clear();
	// log("clear time of gather: %lf\n", MPI_Wtime() - clear);

	// double wait = MPI_Wtime();
	// MPI_Barrier(comm->mpi_comm);
	while(comm->is_receiving());
	// log("gather Wait time: %lf\n", MPI_Wtime() - wait);
}

template<class KeyType, class ValueType>
inline void Engine<KeyType, ValueType>::execute_apply()
{
	Sender<typename MesgBuf::SyncMesg> *sender_low[COMP_THREADS];
	Sender<typename MesgBuf::SyncMesg> *sender_high[COMP_THREADS];
	for (int i = 0; i < COMP_THREADS; ++i)
	{
		sender_low[i] = new Sender<typename MesgBuf::SyncMesg>(comm, SYNC_MIRROR_LOW);
		sender_high[i] = new Sender<typename MesgBuf::SyncMesg>(comm, SYNC_MIRROR_HIGH);
	}

	engine_apply(low_master_active_array, LOW_MASTER, sender_low);
	engine_apply(high_master_active_array, HIGH_MASTER, sender_high);

	double start = MPI_Wtime();
	#pragma omp parallel num_threads(COMP_THREADS)
	{
		if(0 == omp_get_thread_num())
		{
			for (int i = 0; i < COMP_THREADS; ++i)
			{
				sender_low[i]->flush();
				delete sender_low[i];
				sender_high[i]->flush();
				delete sender_high[i];
			}
			MPI_Barrier(comm->mpi_comm);
		}
		else
		{
			while(comm->is_receiving())
				irecv();
		}
	}

	double wait_time = MPI_Wtime() - start;
	apply_comm_time += wait_time;
	log("Apply wait time: %lf\n", wait_time);

	// double wait = MPI_Wtime();
	// MPI_Barrier(comm->mpi_comm);
	// while(comm->is_receiving());
	// log("apply Wait time: %lf\n", MPI_Wtime() - wait);
}

template<class KeyType, class ValueType>
inline void Engine<KeyType, ValueType>::execute_scatter()
{
	Sender<typename MesgBuf::ActMesg> *sender[COMP_THREADS];
	for (int i = 0; i < COMP_THREADS; ++i)
		sender[i] = new Sender<typename MesgBuf::ActMesg>(comm, ACT);

#ifdef SKIP_SCATTER
	unsigned long in = 0, out = 0, local_active_edges = 0, global_active_edges;
	for (int i = 0; i < COMP_THREADS; ++i)
	{
		in += num_in_edges[i];
		out += num_out_edges[i];
	}
	if(IN_EDGES & v_prog->scatter_edge())
		local_active_edges += in;
	if(OUT_EDGES & v_prog->scatter_edge())
		local_active_edges += out * mpi_size; // a rough measurement of the active out edges

	MPI_Allreduce(&local_active_edges, &global_active_edges, 1, MPI_UNSIGNED_LONG, MPI_SUM, comm->mpi_comm);
	log("Note: in: %ld, out:%ld, all:%ld\n", in, out, global_active_edges);
	// more than half of the edges are active (when scatter ALL_EDGES in and out may be counted twice, so a factor 2 is omitted)
	int factor = (v_prog->scatter_edge() == ALL_EDGES) ? 1 : 2;
	if(global_active_edges * factor > graph->num_edges)
	{
		log("Note: scatter skip\n");
		graph->low_active_master.set_all();
		graph->low_active_mirror.set_all();
		graph->high_active_master.set_all();
		graph->high_active_mirror.set_all();
	}
	else
#endif		
	{
		engine_scatter(low_master_active_array, LOW_MASTER, sender);
		engine_scatter(low_mirror_active_array, LOW_MIRROR, sender);
		engine_scatter(high_master_active_array, HIGH_MASTER, sender);
		engine_scatter(high_mirror_active_array, HIGH_MIRROR, sender);
	}
	double start = MPI_Wtime();
	// volatile bool is_receiving = true;
	#pragma omp parallel num_threads(COMP_THREADS)
	{
		if(0 == omp_get_thread_num())
		{
			for (int i = 0; i < COMP_THREADS; ++i)
			{
				sender[i]->flush();
				delete sender[i];
			}
			MPI_Barrier(comm->mpi_comm);
			// is_receiving = false;
		}
		else
		{
			while(comm->is_receiving())
				irecv();
		}
	}

	// MPI_Barrier(comm->mpi_comm);
	// while(comm->is_receiving());
	double wait_time = MPI_Wtime() - start;
	scatter_comm_time += wait_time;
	log("Scatter Wait time: %lf\n", wait_time);

	bitmap_to_array_all();

}

template<class KeyType, class ValueType>
inline void Engine<KeyType, ValueType>::engine_apply(std::pair<KeyType, KeyType*> active_array, VTYPE type, Sender<typename MesgBuf::SyncMesg> *sender[])
{

	// int mesg_num_send[mpi_size];
	// int mesg_num_recv[mpi_size];
	// memset(mesg_num_send, 0, sizeof(int) * mpi_size);	

	double start = MPI_Wtime();

	// low degree

	KeyType array_size = active_array.first;
	KeyType *array = active_array.second;

	// sync to mirror
	// TAG tag = (LOW_MASTER == type) ? SYNC_MIRROR_LOW : SYNC_MIRROR_HIGH;
	// Sender<typename MesgBuf::SyncMesg> *sender[COMP_THREADS];
	// for (int i = 0; i < COMP_THREADS; ++i)
	// 	sender[i] = new Sender<typename MesgBuf::SyncMesg>(comm, tag);


	ValueType *local_buf;
	if(LOW_MASTER == type)
		local_buf = mesg_buf->get_sync_buf_low();
	else if(HIGH_MASTER == type)
		local_buf = mesg_buf->get_sync_buf_high();
	

	#pragma omp parallel for num_threads(COMP_THREADS) schedule(OMP_SCHEDULE_TYPE)
	for (KeyType i = 0; i < array_size; ++i)
	{
		int thread_id = omp_get_thread_num();

		KeyType lid = array[i];
		KeyType gid;
		VertexType *v;
		if(LOW_MASTER == type)
		{
			gid = graph->ltog_low_master[lid];
			v = &(((graph->low_degree_master).find(gid))->second);
		}
		else //if(HIGH_MASTER == type)
		{
			gid = graph->ltog_high_master[lid];
			v = &(((graph->high_degree_master).find(gid))->second);
		}
		// else
		// 	exit(0);

		v_prog->apply(*v, local_buf[lid]);

		// master send sync mesg to mirrors
		if(!v->change)
			continue;
		
		num_in_edges[thread_id] += v->in_nbrs_v.size();
		num_out_edges[thread_id] += v->out_nbrs_v.size();

		auto pair_it = (graph->mirror).find(gid);
		if((graph->mirror.end()) != pair_it)
		{
			auto &mirror_set = pair_it->second;
			for (int mirror_rank : mirror_set)
			{
				// if(mirror_rank == rank)
				// 	continue;
				typename MesgBuf::SyncMesg *bucket= sender[thread_id]->get_bucket(mirror_rank);
				bucket->id = gid;
				bucket->value = v->get_value();
				// mesg_num_send[mirror_rank]++;
			}
		}
		// engine_scatter(v);
	}
	log("Apply update time: %lf\n", MPI_Wtime() - start);

	// start = MPI_Wtime();
	// volatile bool is_receiving = true;
	// #pragma omp parallel num_threads(COMP_THREADS)
	// {
	// 	// int thread_id = omp_get_thread_num();
	// 	// sender[thread_id]->flush();
	// 	// while(!sender[thread_id]->test_all())
	// 	// 	irecv();
	// 	// delete sender[thread_id];
	// 	if(0 == omp_get_thread_num())
	// 	{
	// 		for (int i = 0; i < COMP_THREADS; ++i)
	// 		{
	// 			sender[i]->flush();
	// 			delete sender[i];
	// 		}
	// 		MPI_Barrier(comm->mpi_comm);
	// 		// is_receiving = false;
	// 	}
	// 	else
	// 	{
	// 		while(comm->is_receiving())
	// 			irecv();
	// 	}
	// }

	// double wait_time = MPI_Wtime() - start;
	// apply_comm_time += wait_time;
	// log("Apply wait time: %lf\n", wait_time);

}



template<class KeyType, class ValueType>
inline void Engine<KeyType, ValueType>::engine_gather_master(std::pair<KeyType, KeyType*> active_array, VTYPE type)
{
	KeyType array_size = active_array.first;
	KeyType *array = active_array.second;

	double start = MPI_Wtime();
	ValueType *local_buf;
	
	if(LOW_MASTER == type)
		local_buf = mesg_buf->get_sync_buf_low();
	else //if(HIGH_MASTER == type)
		local_buf = mesg_buf->get_sync_buf_high();
	
	#pragma omp parallel for num_threads(COMP_THREADS) schedule(OMP_SCHEDULE_TYPE)
	for (KeyType i = 0; i < array_size; i++)
	{
		KeyType lid = array[i];
		KeyType gid;
		VertexType *v;
		ValueType acc = acc_init;
		
		// start = MPI_Wtime();
		if(LOW_MASTER == type)
		{
			gid = graph->ltog_low_master[lid];
			v = &(((graph->low_degree_master).find(gid))->second);
		}
		else //if(HIGH_MASTER == type)
		{
			gid = graph->ltog_high_master[lid];
			v = &(((graph->high_degree_master).find(gid))->second);
		}
		// else
		// 	exit(0);
		v->is_active = false;

		// log("search time: %lf\n", MPI_Wtime() - start);
		// start = MPI_Wtime();

		if(IN_EDGES & v_prog->gather_edge(*v))
		{
			// std::vector<KeyType> *nbrs;
			// nbrs = &(v->get_in_nbr());
			for(auto nbr_v : v->in_nbrs_v)
			{
				// double find_time = MPI_Wtime();
				// auto &nbr_v = graph->find_vertex(nbr_id);
				// log("Note: find_time: %.10lf\n", MPI_Wtime() - find_time);
				acc = v_prog->op(v_prog->gather(*v, *nbr_v), acc);
			}
		}
		if(OUT_EDGES & v_prog->gather_edge(*v))
		{
			// std::vector<KeyType> *nbrs;
			// nbrs = &(v->get_out_nbr());
			for(auto nbr_v : v->out_nbrs_v)
			{
				// auto &nbr_v = graph->find_vertex(nbr_id);
				acc = v_prog->op(v_prog->gather(*v, *nbr_v), acc);
			}
		}
		// if(!v_prog->gather_edge())
		// 	acc = v_prog->op(v_prog->gather(*v, *v), acc); // the second *v should never be used

		
		local_buf[lid] = acc;
	}
	log("MASTER gather Time: %lf\n", MPI_Wtime() - start);
}

template<class KeyType, class ValueType>
inline void Engine<KeyType, ValueType>::engine_gather_mirror(std::pair<KeyType, KeyType*> active_array, VTYPE type)
{
	if((IN_EDGES == v_prog->gather_edge()) && (type == LOW_MIRROR))
		return;

	KeyType array_size = active_array.first;
	KeyType *array = active_array.second;

	TAG tag = (LOW_MIRROR == type) ? SYNC_MASTER_LOW : SYNC_MASTER_HIGH;
	Sender<typename MesgBuf::SyncMesg> *sender[COMP_THREADS];
	for (int i = 0; i < COMP_THREADS; ++i)
		sender[i] = new Sender<typename MesgBuf::SyncMesg>(comm, tag);
	
	double start = MPI_Wtime();
	#pragma omp parallel for num_threads(COMP_THREADS) schedule(OMP_SCHEDULE_TYPE)
	for (KeyType i = 0; i < array_size; i++)
	{
		int thread_id = omp_get_thread_num();

		KeyType lid = array[i];
		KeyType gid;
		VertexType *v;
		ValueType acc = acc_init;
		// start = MPI_Wtime();

		if(LOW_MIRROR == type)
		{
			// if(graph->ltog_low_mirror.find(lid) == graph->ltog_low_mirror.end())
			// 	log("rank: %d, Error: low mirror lid %d not found\n", rank, lid);
			gid = graph->ltog_low_mirror[lid];
			v = &(((graph->low_degree_mirror).find(gid))->second);
		}
		else //if(HIGH_MIRROR == type)
		{
			// if(graph->ltog_high_mirror.find(lid) == graph->ltog_high_mirror.end())
			// 	log("rank: %d, Error: high mirror lid %d not found\n", rank, lid);
			gid = graph->ltog_high_mirror[lid];
			v = &(((graph->high_degree_mirror).find(gid))->second);
		}
		// else
		// 	exit(0);
		v->is_active = false;

		// log("search time: %lf\n", MPI_Wtime() - start);
		// start = MPI_Wtime();
		if((IN_EDGES & v_prog->gather_edge(*v)))
		{
			// std::vector<KeyType> *nbrs;
			// nbrs = &(v->get_in_nbr());
			for(auto nbr_v : v->in_nbrs_v)
			{
				// auto &nbr_v = graph->find_vertex(nbr_id);
				acc = v_prog->op(v_prog->gather(*v, *nbr_v), acc);
			}
		}
		if(OUT_EDGES & v_prog->gather_edge(*v))
		{
			// std::vector<KeyType> *nbrs;
			// nbrs = &(v->get_out_nbr());
			for(auto nbr_v : v->out_nbrs_v)
			{
				// auto &nbr_v = graph->find_vertex(nbr_id);
				acc = v_prog->op(v_prog->gather(*v, *nbr_v), acc);
			}
		}
		// if(!v_prog->gather_edge())
		// 	acc = v_prog->op(v_prog->gather(*v, *v), acc); // the second *v should never be used
		// log("nbrs gathering time: %lf\n", MPI_Wtime() - start);
		// start = MPI_Wtime();
		int target_rank = graph->hash(gid);
		typename MesgBuf::SyncMesg *bucket = sender[thread_id]->get_bucket(target_rank);
		bucket->id = gid;
		bucket->value = acc;
	
	}
	log("MIRROR gather compute Time: %lf\n", MPI_Wtime() - start);

	start = MPI_Wtime();
	// volatile bool is_receiving = true;
	#pragma omp parallel num_threads(COMP_THREADS)
	{
		if(0 == omp_get_thread_num())
		{
			for (int i = 0; i < COMP_THREADS; ++i)
			{
				sender[i]->flush();
				delete sender[i];
			}
			MPI_Barrier(comm->mpi_comm);
			// is_receiving = false;
		}
		else
		{
			while(comm->is_receiving())
				irecv();
		}
	}
	double wait_time = MPI_Wtime() - start;
	gather_comm_time += wait_time;
	log("MIRROR gather wait time: %lf\n", wait_time);

}



template<class KeyType, class ValueType>
inline void Engine<KeyType, ValueType>::engine_scatter(std::pair<KeyType, KeyType*> active_array, VTYPE type, Sender<typename MesgBuf::ActMesg> *sender[])
{
	KeyType array_size = active_array.first;
	KeyType *array = active_array.second;

	// int num_in_nbrs[COMP_THREADS];
	// int num_out_nbrs[COMP_THREADS];
	// for (int i = 0; i < COMP_THREADS; ++i)
	// {
	// 	num_in_nbrs[i] = 0;
	// 	num_out_nbrs[i] = 0;
	// }

	#pragma	omp parallel for num_threads(COMP_THREADS)
	for (KeyType i = 0; i < array_size; ++i)
	{
		int thread_id = omp_get_thread_num();

		KeyType lid = array[i];
		KeyType gid;
		VertexType *v;
		switch(type)
		{
			case LOW_MASTER: 
			{
				gid = graph->ltog_low_master[lid];
				v = &(((graph->low_degree_master).find(gid))->second);
				break;
			}
			case LOW_MIRROR:
			{
				gid = graph->ltog_low_mirror[lid];
				v = &(((graph->low_degree_mirror).find(gid))->second);
				break;
			}
			case HIGH_MASTER:
			{
				gid = graph->ltog_high_master[lid];
				v = &(((graph->high_degree_master).find(gid))->second);
				break;
			}
			case HIGH_MIRROR:
			{
				gid = graph->ltog_high_mirror[lid];
				v = &(((graph->high_degree_mirror).find(gid))->second);
				break;
			}
			default: log("Wrong type\n"); exit(0);
		}
		// num_in_nbrs[thread_id] += v->get_in_nbr().size();
		// num_out_nbrs[thread_id] += v->get_out_nbr().size();

		if(IN_EDGES & v_prog->scatter_edge(*v))
		{
			// std::vector<KeyType> *nbrs;
			// nbrs = &(v->get_in_nbr());
			for (auto nbr_v : v->in_nbrs_v)
			{
				// auto &nbr_v = graph->find_vertex(nbr);
				KeyType nbr_id = nbr_v->get_id();
				if(v_prog->scatter(*v, *nbr_v))
				{
					// auto &nbr_v = graph->find_vertex(nbr);
					int nbr_rank = graph->hash(nbr_id);
					if(rank == nbr_rank)
					{
						if(nbr_v->is_active || graph->insert_active_master(nbr_id))
							continue;
						nbr_v->is_active = true;
						auto pair_it = (graph->mirror).find(nbr_id);
						if((graph->mirror).end() != pair_it)
						{
							auto &mirror_set = pair_it->second;
							for (int mirror_rank : mirror_set)
							{
								typename MesgBuf::ActMesg *bucket = sender[thread_id]->get_bucket(mirror_rank);
								bucket->id = nbr_id;
							}
						}
					}
					else
					{
						if(nbr_v->is_active || graph->insert_active_mirror(nbr_id))
							continue;
						nbr_v->is_active = true;
						for (int target_rank = 0; target_rank < mpi_size; ++target_rank)
						{
							if(target_rank == rank)
								continue;
							typename MesgBuf::ActMesg *bucket = sender[thread_id]->get_bucket(target_rank);
							bucket->id = nbr_id;
						}
					}
				}
			}
		}
		if(OUT_EDGES & v_prog->scatter_edge(*v))
		{
			// std::vector<KeyType> *nbrs;
			// nbrs = &(v->get_out_nbr());
			for (auto nbr_v : v->out_nbrs_v)
			{
				// auto &nbr_v = graph->find_vertex(nbr);
				KeyType nbr_id = nbr_v->get_id();
				if(v_prog->scatter(*v, *nbr_v))
				{
					// double hash_time = MPI_Wtime();
					int nbr_rank = graph->hash(nbr_id);
					// log("Note: hash_time: %.10lf\n", MPI_Wtime() - hash_time);
					if(rank == nbr_rank)
					{
						if(nbr_v->is_active || graph->insert_active_master(nbr_id))
							continue;
						nbr_v->is_active = true;
						auto pair_it = (graph->mirror).find(nbr_id);
						if((graph->mirror).end() != pair_it)
						{
							auto &mirror_set = pair_it->second;
							for (int mirror_rank : mirror_set)
							{
								typename MesgBuf::ActMesg *bucket = sender[thread_id]->get_bucket(mirror_rank);
								bucket->id = nbr_id;
								// log("Note:rank: %d, mirror_rank: %d, master act id:%d nbr:%d\n", rank, mirror_rank, v->get_id(), bucket->id);
							}
						}
					}
					else
					{
						if(nbr_v->is_active || graph->insert_active_mirror(nbr_id))
							continue;
						nbr_v->is_active = true;
						for (int target_rank = 0; target_rank < mpi_size; ++target_rank)
						{
							if(target_rank == rank)
								continue;
							typename MesgBuf::ActMesg *bucket = sender[thread_id]->get_bucket(target_rank);
							bucket->id = nbr_id;
							// log("Note:rank: %d, mirror_rank: %d, mirror act id:%d nbr:%d\n", rank, target_rank, v->get_id(), nbr);
						}
					}
				}
			}
		}
	}

	// int in = 0, out = 0;
	// for (int i = 0; i < COMP_THREADS; ++i)
	// {
	// 	in += num_in_nbrs[i];
	// 	out += num_out_nbrs[i];
	// }
	// log("Note: in: %d, out:%d\n", in, out);
}

template<class KeyType, class ValueType>
inline void Engine<KeyType, ValueType>::bitmap_to_array_all()
{
	double transfer = MPI_Wtime();
	#pragma omp parallel sections num_threads(4)
	{
		#pragma omp section
		{
			bitmap_to_array(graph->low_active_master, low_master_active_array);
		}
		#pragma omp section
		{
			bitmap_to_array(graph->low_active_mirror, low_mirror_active_array);
		}
		#pragma omp section
		{
			bitmap_to_array(graph->high_active_master, high_master_active_array);
		}
		#pragma omp section
		{
			bitmap_to_array(graph->high_active_mirror, high_mirror_active_array);
		}
	}
	log("transfer time: %lf\n", MPI_Wtime() - transfer);
}

template<class KeyType, class ValueType>
inline void Engine<KeyType, ValueType>::bitmap_to_array(BitMap &active_set, std::pair<KeyType, KeyType*> &active_array)
{
	size_t index = 0;

	size_t id;
	while(active_set.next_bit(&id))
	{
		// active_array[index] = ltog[(KeyType)id];
		active_array.second[index] = (KeyType)id;
		index++;
	}
	active_set.set_size(index);
	active_array.first = (KeyType)index;

}

template<class KeyType, class ValueType>
inline void Engine<KeyType, ValueType>::irecv()
{
	int tag;
	void *recv_buf = NULL;
	if(comm->irecv(&tag, &recv_buf))
		mesg_buf->process_mesg(tag, recv_buf);
}

#endif

