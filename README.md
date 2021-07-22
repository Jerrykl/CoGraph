# CoGraph

CoGraph is a distributed graph computing engine for graph analysis and processing. Based on the computing model of PowerLyra, CoGraph optimizes the communication model and provides the same GAS (Gather, Apply, Scatter) programming interfaces for users as PowerLyra and PowerGraph. Graph processing algorithms such as PageRank, CC (connected component) and SSSP (single source shortest path) could be implemented by GAS programming interfaces.

## Compilation

- Prerequisites: C++11, MPI (MPICH or OpenMPI), OpenMP

There are several example applications (pagerank, sssp, cc) been provided under directory src. To compile them, run the following commands.

```
cd src
make
```

For custom applications, append the program to the `TARGET` variable in the Makefile.

## Dataset

CoGraph takes binary graph file as input (datasets such as those from SNAP should be transformed to binary format beforehand, the script helper/converse.py could be used for transformation). The graph format is as follow, where each line represents an edge.

```
0 1
1 2
1 3
2 3
3 4
```

## Running

To run CoGraph on distributed environment, use `mpirun` and specify the servers, the application to be run and the graph dataset.

```
mpirun [-hosts serverlist] [application] -g [dataset]
```

An example of running pagerank with four servers on graph twitter.bin is shown as follow.

```
mpirun -hosts slave1,slave3,slave4,slave5 ./pagerank -g ./twitter.bin
```

## Custom application

Users can refer to the existing applications such as PageRank to customize a class that inherits `VertexProgram`, and implement the virtual functions to write custom graph applications.
