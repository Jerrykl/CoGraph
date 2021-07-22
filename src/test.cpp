
//#define MPI_DEBUG
#include "controller.hpp"
#include "graph.hpp"

int main(int argc, char const *argv[])
{
#ifdef MPI_DEBUG
	volatile int gdb_break = 1;
	while(gdb_break);
#endif

	Controller<int, int> controller(argc, argv);

	char const *file_path = argv[1];
	Graph<int, int> graph = Graph<int, int>(&controller, file_path, 200, 1);

	// int a = ((graph.get_low_degree_v().find(0))->second).get_nbrs().size();
	// log("size: %d\n", a);

	
	return 0;
}