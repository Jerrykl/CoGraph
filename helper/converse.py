
import struct

path = input().strip()

basename, suffix = path.split('.')
new_file = basename + ".bin"

with open(path, 'r') as file:
	lines = file.readlines()

with open(new_file, 'wb+') as file:
	for line in lines:
		if line[0]=='#':
			continue
		src, dst = tuple(map(int, line.split()))	
		binary = struct.pack("2i", src, dst)
		file.write(binary)
