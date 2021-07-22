
MPICXX = mpic++
CXXFLAGS = -O3 -Wall -g -std=c++11 -lstdc++ -fopenmp -lpthread

TARGETS= pagerank cc sssp kcore
HEADERS = controller.hpp communicator.hpp graph.hpp vertex.hpp log.h message.hpp worker.hpp engine.hpp vertexprogram.hpp bitmap.hpp sender.hpp

SOURCE=$(wildcard *.cpp)
OBJECTS=$(SOURCE:%.cpp=%.o)

all: $(TARGETS)

%.o: %.cpp $(HEADERS)
	$(MPICXX) $(CXXFLAGS) -c $< -o $@

$(TARGETS): %:%.o
	$(MPICXX) $(CXXFLAGS) $< -o $@

.PHONY: clean
clean:
	rm -f $(TARGETS) *.o
