
#ifndef WORKER
#define WORKER


#include <vector>
#include <omp.h>
#include "message.hpp"
#include "communicator.hpp"

template<class KeyType, class ValueType>
class Worker
{
private:
	Communicator *comm;
	MessageBuffer<KeyType, ValueType> *mesg_buf;

	bool loop;
public:
	Worker(Communicator *comm, MessageBuffer<KeyType, ValueType> *mesg_buf):comm(comm), mesg_buf(mesg_buf)
	{
		loop = true;
	}
	~Worker() {}

	static void *run(void *arg);
	inline void stop() {loop = false;}
};

template<class KeyType, class ValueType>
void *Worker<KeyType, ValueType>::run(void *arg)
{
	Worker<KeyType, ValueType> *worker = (Worker<KeyType, ValueType> *)arg;
	#pragma omp parallel num_threads(worker->comm->num_threads)
	{
		while(worker->loop)
		{
			int tag;
			void *recv_buf = NULL;
			(worker->comm)->recv_loop(worker->loop, &tag, &recv_buf);
			(worker->mesg_buf)->process_mesg(tag, recv_buf);
		}

	}
	return NULL;
}

#endif