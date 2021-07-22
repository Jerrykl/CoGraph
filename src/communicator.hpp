
#ifndef COMMUNICATOR
#define COMMUNICATOR

#include <mpi.h>
#include <unistd.h>
#include <string.h>
#include "message.hpp"

#include <sys/sysinfo.h>
// number of threads
const int TOTAL_THREADS = get_nprocs()/2;
// number of communication threads
const int COMM_THREADS = 4;
// number of computing threads
const int COMP_THREADS = std::max(TOTAL_THREADS - COMM_THREADS, COMM_THREADS);
// omp schedule strategy
#define OMP_SCHEDULE_TYPE guided

/*
	MPI Communicator
*/

class Communicator
{
private:
	int size;
	int rank;

public:
	const MPI_Comm mpi_comm = MPI_COMM_WORLD;
	Communicator() {};
	~Communicator() {MPI_Finalize();}

	void init();

	inline int get_size() const {return size;}
	inline int get_rank() const {return rank;}

	void recv_loop(bool &loop, int *tag, void **recv_buf);
	inline void send(int target_rank, int tag, void *buf, int size);
	inline void ssend(int target_rank, int tag, void *buf, int size);
	inline void issend(int target_rank, int tag, void *buf, int size, MPI_Request *req);
	inline int irecv(int *tag, void **recv_buf);
	
	static const int num_threads = COMM_THREADS;
	volatile int receiving[num_threads] = {0};
	inline bool is_receiving()
	{
		for (int i = 0; i < num_threads; ++i)
			if(receiving[i])
				return true;
		return false;
	}

};

void Communicator::init()
{
	MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, NULL);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

}

void Communicator::recv_loop(bool &loop, int *tag, void **recv_buf)
{
	int thread_id = omp_get_thread_num();

	MPI_Message msg;
	MPI_Status status;
	int return_code;

	int flag = 0;
	int recv_size;

	while(!flag)
	{
		MPI_Improbe(MPI_ANY_SOURCE, MPI_ANY_TAG, mpi_comm, &flag, &msg, &status);
		receiving[thread_id] = flag;
		if(!loop)
			exit(0);
		if(!flag)
			usleep(10);
	}
	MPI_Get_count(&status, MPI_BYTE, &recv_size);
	*recv_buf = (void *)malloc(recv_size);
	memset(*recv_buf, 0, recv_size);

	return_code = MPI_Mrecv(*recv_buf, recv_size, MPI_BYTE, &msg, &status);
	if(MPI_SUCCESS != return_code)
		log("MPI_MRecv failed\n");
	*tag = status.MPI_TAG;

}

inline int Communicator::irecv(int *tag, void **recv_buf)
{
	MPI_Message msg;
	MPI_Status status;
	int return_code;

	int flag = 0;
	int recv_size;

	MPI_Improbe(MPI_ANY_SOURCE, MPI_ANY_TAG, mpi_comm, &flag, &msg, &status);
	MPI_Get_count(&status, MPI_BYTE, &recv_size);
	if(!flag)
		return flag;
	*recv_buf = (void *)malloc(recv_size);
	memset(*recv_buf, 0, recv_size);

	return_code = MPI_Mrecv(*recv_buf, recv_size, MPI_BYTE, &msg, &status);
	if(MPI_SUCCESS != return_code)
		log("MPI_MRecv failed\n");
	*tag = status.MPI_TAG;
	return flag;
}


inline void Communicator::send(int target_rank, int tag, void *buf, int size)
{
	int return_code = MPI_Send(buf, size, MPI_BYTE, target_rank, tag, mpi_comm);
	if(MPI_SUCCESS != return_code)
		log("MPI_Send failed");
}

/*
	synchronous send
*/
inline void Communicator::ssend(int target_rank, int tag, void *buf, int size)
{
	int return_code = MPI_Ssend(buf, size, MPI_BYTE, target_rank, tag, mpi_comm);
	if(MPI_SUCCESS != return_code)
		log("MPI_Send failed");
}

inline void Communicator::issend(int target_rank, int tag, void *buf, int size, MPI_Request *req)
{
	int return_code = MPI_Issend(buf, size, MPI_BYTE, target_rank, tag, mpi_comm, req);
	if(MPI_SUCCESS != return_code)
		log("MPI_ISend failed");
}


#endif