

#ifndef SENDER
#define SENDER

template<class MesgType>
class Sender
{
private:
	typedef struct
	{
		MesgType * mesg;
		int len;
	}TableEntry;

	const int block_len = 1000;
	Communicator *comm;
	int size;
	int tag;
	TableEntry *table;
	std::vector<MesgType *> sent_block;
	std::vector<MPI_Request> requests;

public:
	Sender(Communicator *comm, int tag):comm(comm), tag(tag)
	{
		size = comm->get_size();
		table = new TableEntry[size];
		memset(table, 0, sizeof(TableEntry) * size);
	}
	~Sender() {delete []table;}

	inline MesgType *get_bucket(int target);
	inline void flush();
	inline int test_all();
};


template<class MesgType>
inline MesgType *Sender<MesgType>::get_bucket(int target)
{
	MesgType *block_ptr = table[target].mesg;
	if(!block_ptr)
	{
		block_ptr = (MesgType *)malloc(sizeof(MesgType) * block_len);
		table[target].mesg = block_ptr;
		table[target].len = 1;
	}
	else
		if(block_len == table[target].len)
		{
			(table[target].mesg)->id = block_len - 1;
			sent_block.push_back(table[target].mesg);
			MPI_Request req;
			comm->issend(target, tag, table[target].mesg, sizeof(MesgType) * block_len, &req);
			requests.push_back(req);

			block_ptr = (MesgType *)malloc(sizeof(MesgType) * block_len);
			table[target].mesg = block_ptr;
			table[target].len = 1;
		}
	return block_ptr + table[target].len++;
}

template<class MesgType>
inline void Sender<MesgType>::flush()
{
	for (int target = 0; target < size; ++target)
	{
		if(table[target].mesg != NULL)
		{
			table[target].mesg->id = table[target].len - 1;
			comm->ssend(target, tag, table[target].mesg, sizeof(MesgType) * block_len);
			free(table[target].mesg);			
		}
	}
	MPI_Waitall(requests.size(), &requests[0], MPI_STATUSES_IGNORE);
	for(auto block_ptr : sent_block)
		free(block_ptr);
}

template<class MesgType>
inline int Sender<MesgType>::test_all()
{
	int flag;
	MPI_Testall(requests.size(), &requests[0], &flag, MPI_STATUSES_IGNORE);
	if(flag)
	{
		for(auto block_ptr : sent_block)
			free(block_ptr);
	}
	return flag;
}

#endif

// template<class MesgType>
// inline void Sender<MesgType>::flush()
// {
// 	for (int target = 0; target < size; ++target)
// 	{
// 		if(table[target].mesg != NULL)
// 		{
// 			table[target].mesg->id = table[target].len - 1;
// 			sent_block.push_back(table[target].mesg);
// 			MPI_Request req;
// 			comm->issend(target, tag, table[target].mesg, sizeof(MesgType) * block_len, &req);
// 			requests.push_back(req);
// 		}
// 	}
	
// }