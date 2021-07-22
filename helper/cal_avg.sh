
dir=$1

for file in `ls $dir`
do
	echo $file
	cat $1"/"$file/* | grep computing | awk '{print $4}' | awk '{sum+=$1} END {print "Avg =", sum/NR}'
done