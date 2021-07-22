
#include<unordered_map>
#include<fstream>

using namespace std;

int main(int argc, char const *argv[])
{

	ifstream fin(argv[1]);
	ofstream fout(argv[2]);

	unordered_map<long, unordered_map<long, bool>> edges;

	long src, dst;
	while (fin>>src>>dst) {
		if (src > dst) {
			swap(src, dst);
		}
		if (edges[src][dst]) {
			continue;
		}
		edges[src][dst] = true;

		fout<<src<<" "<<dst<<endl;
	}

	return 0;
}