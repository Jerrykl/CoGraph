#!/bin/bash

export OMP_WAIT_POLICY=PASSIVE
export OMP_PROC_BIND=false
export OMP_PLACES=cores

# parameter
# $1 dataset

result_dir="../result/"
dataset_dir="../../data/"

dataset=$1

nodes=node6,node8,node9,node11,node14,node17
nodes_array=(${nodes//,/ })
num_nodes=${#nodes_array[@]}

threshold=1000
sssp_source=12

programs=(pagerank cc sssp)

for program in ${programs[@]}
do
	dir=$result_dir$program"-"$dataset-$num_nodes
	mkdir $dir
	for i in {1..10}
	do
		mpirun -hosts $nodes "./"$program $dataset_dir$dataset".bin" $threshold $sssp_source > $dir"/"$i".txt"
		echo $dir"/"$i".txt"
	done
done
