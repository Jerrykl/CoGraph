
#ifndef CONTROLLER
#define CONTROLLER

#include "communicator.hpp"
#include "worker.hpp"
#include "message.hpp"
#include "log.h"
#include <pthread.h>

template<class KeyType, class ValueType>
class Controller
{
private:
	pthread_t worker_thread;
public:
	Worker<KeyType, ValueType> *worker;
	Communicator *comm;
	MessageBuffer<KeyType, ValueType> *mesg_buf;
	Controller()
	{
		comm = new Communicator();
		comm->init();

		mesg_buf = new MessageBuffer<KeyType, ValueType>(comm->get_rank());
		worker = new Worker<KeyType, ValueType>(comm, mesg_buf);

		pthread_create(&worker_thread, NULL, Worker<KeyType, ValueType>::run, worker);

	}
	~Controller()
	{
		worker->stop();
		pthread_join(worker_thread, NULL);
		delete worker;
		delete mesg_buf;
		delete comm;
	}

	
};

#endif
