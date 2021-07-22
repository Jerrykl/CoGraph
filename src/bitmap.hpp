

class BitMap
{
private:
	uint8_t *array = NULL; // bit_map: 0 is inactive, 1 is active
	size_t num_bits;
	size_t arr_len;
	size_t count; // number of ones
	size_t unit_len = sizeof(uint8_t);
	size_t arr_index;
	size_t bit_index;
public:
	BitMap () {}

	~BitMap()
	{
		if(array != NULL)
			free(array);
	}

	void init(size_t total_bits)
	{
		num_bits = total_bits;
		arr_len = int(total_bits / unit_len) + 1;
		array = (uint8_t *)malloc(arr_len * unit_len);
		clear();
	}
	inline bool set_bit(size_t id);
	inline bool next_bit(size_t *id);
	inline void clear();
	inline size_t size();

	inline void set_all();
	inline void set_size(size_t size);

	inline void set_count_to_zero() {count = 0;}

};

inline bool BitMap::set_bit(size_t id)
{
	// size_t local_id = (graph->global_to_local)[id];
	size_t arrpos = size_t(id / unit_len);
	size_t bitpos = id % unit_len;
	uint8_t mask = 1 << bitpos;
	return __sync_fetch_and_or(array + arrpos, mask) & mask;
}

inline bool BitMap::next_bit(size_t *id)
{
	for (; arr_index < arr_len; ++arr_index)
	{
		uint8_t unit = array[arr_index];
		if(!unit)
			continue;
		for (; bit_index < unit_len; ++bit_index)
		{
			if(1 & (unit >> bit_index))
			{
				*id = arr_index * unit_len + bit_index;
				if(++bit_index == unit_len)
				{
					arr_index++;
					bit_index = 0;
				}
				else
					bit_index++;
				return true;
			}
		}
	}
	return false;
}

inline void BitMap::clear()
{
	count = 0;
	arr_index = 0;
	bit_index = 0;
	memset(array, 0, arr_len * unit_len);
}

inline void BitMap::set_all()
{
	memset(array, 0xff, (arr_len - 1) * unit_len);
	for (size_t i = 0; i < num_bits % unit_len; ++i)
		array[arr_len-1] |= 1 << i;
}

inline void BitMap::set_size(size_t size)
{
	count = size;
}

inline size_t BitMap::size()
{
	if(count)
		return count;
	for (size_t i = 0; i < arr_len; ++i)
	{
		uint8_t unit = array[i];
		if(!unit)
			continue;
		for (size_t j = 0; j < unit_len; ++j)
		{
			if(1 & (unit >> j))
				count++;
		}
	}
	return count;
}