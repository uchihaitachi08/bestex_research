#include <iostream>
#include <vector>
#include <string>
#include <sys/stat.h>
#include <dirent.h>
#include <algorithm>
#include <ctime>
#include <fstream>
#include <sstream>
#include <unordered_map>
#include <queue>
#include <string.h>

//for mmap
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

/*! @brief Stating the clock to measure time */
const clock_t begin_time = clock();

/*! @brief Get page mask for the memory mapped files */
std::size_t getPageMask()
{
	return ~(sysconf(_SC_PAGE_SIZE) - 1);
}

/*! @brief Round up for the memory mapped files according to the page size */
std::size_t getRoundUpSize(const std::size_t size)
{
	std::size_t _page_mask = getPageMask();
	return (size + ~_page_mask) &_page_mask;
}

/*! @brief Function to handle error message from memory mapping */
void handleError(const char *msg)
{
	perror(msg);
	exit(255);
}

/*! @brief Generate the ram size of the system */
long int findRamSize()
{
	std::string token;
	std::ifstream file("/proc/meminfo");
	while (file >> token)
	{
		if (token == "MemTotal:")
		{
			unsigned long mem;
			if (file >> mem)
				return mem;
			else
				return 0;
		}
	}

	return 0;
}

/*! @brief Stores raw symbol files in vector */
void getSymbolFileNames(std::vector<std::string > &files, std::string &file_path)
{
	DIR * dir;
	struct dirent * diread;

	if ((dir = opendir(file_path.c_str())) != nullptr)
	{
		while ((diread = readdir(dir)) != nullptr)
		{
			std::string temp(diread->d_name);
			if (temp.find("txt") != std::string::npos)
			{
				std::string file_name = diread->d_name;
				std::size_t found = file_name.find(".txt");
				files.push_back(file_name.substr(0, found));

			}
		}

		closedir(dir);
	}
	else
	{
		perror("opendir");
	}
}

/*! @brief Returns file size */
int getFileSize(const std::string &file_name)
{
	struct stat sb {};
	if (!stat(file_name.c_str(), &sb))
	{
		return static_cast<int> (sb.st_size);
	}
	else
	{
		perror("stat");
	}

	return 0;

}

const std::string getLine(std::ifstream &input, std::vector<std::string > files, int i)
{
	std::string sentence;
	getline(input, sentence);
	if (sentence.find("2021") != std::string::npos)
		sentence = sentence.substr(0, 24) + files[i] + "," + sentence.substr(24);
	return sentence;
}

/*! @brief Memory mapping of the raw symbol files 
//  a. Maps raw files to the virtual memory
//  b. From the mapped raw files, pushes strings into the priority queue to sort 
//	c. Dump the sorted output data to run_files
 */
void initMergeMemoryMap(const std::vector<std::string > &files, const std::string &file_path, long int &ram_size)
{
	int curr_memory = 0;
	int file_index = 0;
	int output_count = 0;
	ram_size = static_cast<int> (0.8 *ram_size);
	while (1)
	{
		int file_to_open = 0;
		int file_index_start = file_index;
		curr_memory = 0;
		while (file_index < files.size() && curr_memory < ram_size)
		{
			curr_memory += getFileSize(file_path + files[file_index++] + ".txt");
			file_to_open++;
		}

		//Creating output file before mapping into the memory as memory mapping can't be done to empty files 
		std::stringstream output_file;
		output_file << file_path << "output_" << output_count << ".dat";
		output_count++;
		std::ofstream output_object;
		output_object.open(output_file.str().c_str());
		output_object << "Output file " << std::endl;
		output_object.close();
		int _output_filed = open(output_file.str().c_str(), O_CREAT | O_RDWR);

		if (_output_filed < 0)
		{
			std::cerr << "error opening output file" << output_file.str() << std::endl;
			abort();
		}

		struct stat sbo;
		if (fstat(_output_filed, &sbo) == -1)
			std::cerr << "Error finding size " << std::endl;

		std::size_t length_output = sbo.st_size;
		char *output_buffer = nullptr;
		output_buffer = reinterpret_cast<char*> (mmap(NULL, getRoundUpSize(curr_memory) *2, PROT_WRITE, MAP_SHARED, _output_filed, 0 u));
		if (output_buffer == MAP_FAILED)
		{
			std::cerr << "mapping failed " << std::endl;
		}

		std::vector<char*> input_buffer(file_to_open);
		std::vector<std::string > input_buff(file_to_open);

		for (int i = 0; i < file_to_open; i++)
		{
			std::string file_name = file_path + files[file_index_start + i] + ".txt";
			int _input_filed = open(file_name.c_str(), O_RDONLY);

			if (_input_filed < 0)
			{
				std::cerr << "Error opening file!" << std::endl;
				abort();
			}

			struct stat sb;

			if (fstat(_input_filed, &sb) == -1)
				std::cerr << "Error finding size " << std::endl;

			std::size_t length = sb.st_size;
			int buffer_size = getpagesize();
			input_buffer[i] = reinterpret_cast<char*> (mmap(NULL, length, PROT_READ, MAP_PRIVATE, _input_filed, 0 u));
			if (input_buffer[i] == MAP_FAILED)
			{
				std::cerr << " Error mapping input files to memory " << std::endl;
			}

			input_buff[i] = input_buffer[i];

			if (munmap(input_buffer[i], length) != 0)
			{
				std::cerr << "Error unmapping file memory " << std::endl;
				abort();
			}

			if (close(_input_filed) != 0)
				std::cerr << "Error closing the input file" << std::endl;
		}

		std::priority_queue<std::pair<std::string, int>, std::vector< std::pair<std::string, int>>, std::greater<std::pair<std::string, int>>> pq;
		std::string sentence;
		for (int i = 0; i < file_to_open;)
		{
			std::size_t found = input_buff[i].find('\n');
			if (found == std::string::npos)
				sentence = input_buff[i];
			else
				sentence = input_buff[i].substr(0, found);
			input_buff[i] = input_buff[i].substr(found + 1);
			if (sentence.find("2021") == std::string::npos)
				continue;
			if (sentence.size())
			{
				sentence = sentence.substr(0, 24) + files[file_index_start + i] + "," + sentence.substr(24);
				pq.push(make_pair(sentence, file_index_start + i));
			}

			i++;
		}

		std::string output_buff = "";
		int output_buffer_size = 0;
		while (!pq.empty())
		{
			std::string output_sentence = pq.top().first;
			int index_popped = pq.top().second - file_index_start;
			pq.pop();

			output_buff += output_sentence + '\n';

			//	Dumping output buffer once it become 20% of the ram size 
			if (output_buffer.size() >= ram_size / 5)
			{
				ftruncate(_output_filed, output_buffer_size + output_buff.size());
				strcpy(output_buffer + output_buffer_size, output_buff.c_str());
				output_buffer_size += output_buff.size() + 1;
				output_buff.clear();
			}

			if (input_buff[index_popped].size())
			{
				std::string temp;
				std::size_t found = input_buff[index_popped].find('\n');
				if (found == std::string::npos)
					temp = input_buff[index_popped];
				else
					temp = input_buff[index_popped].substr(0, found);
				input_buff[index_popped] = input_buff[index_popped].substr(found + 1);
				if (temp.find("2021") != std::string::npos && temp.size())
				{
					temp = temp.substr(0, 24) + files[file_index_start + index_popped] + "," + temp.substr(24);
					pq.push(make_pair(temp, file_index_start + index_popped));
				}
			}
		}

		//writing to the output memory file
		ftruncate(_output_filed, output_buffer_size + output_buff.size());
		strcpy(output_buffer + output_buffer_size, output_buff.c_str());
		output_buffer_size += output_buff.size();
		if (msync(output_buffer, getRoundUpSize(curr_memory), MS_SYNC) < 0)
			std::cerr << "error writing into the output file " << std::endl;

		if (munmap(output_buffer, getRoundUpSize(curr_memory)) != 0)
		{
			std::cerr << "Error unmapping output file memory " << std::endl;
			abort();
		}

		if (ftruncate(_output_filed, output_buffer_size) != 0)
			std::cerr << " Error truncating output file " << std::endl;
		if (close(_output_filed) != 0)
			std::cerr << "Error closing the output file" << std::endl;

		if (file_index >= files.size())
			break;
	}

	return;
}

/*! @brief Inefficient function to read from files and write the data to output 
//	Reading and writing raw symbol files in blocks
*/
void initialMergeBlock(const std::vector<std::string > &files, const std::string &file_path, int &ram_size)
{
	int curr_memory = 0;
	int file_index = 0;
	int output_count = 0;
	ram_size = static_cast<int> (0.9 *ram_size);
	while (1)
	{
		int file_to_open = 0;
		while (file_index < files.size() && curr_memory < ram_size)
		{
			curr_memory += getFileSize(file_path + files[file_index++]);
			file_to_open++;
		}

		std::vector<std::string > buff_str(file_to_open);
		for (int i = 0; i < file_to_open; i++)
		{
			std::string file_name = file_path + files[i];
			std::ifstream is(file_name, std::ifstream::binary);
			if (is)
			{
				// get length of file:
				is.seekg(0, is.end);
				int length = is.tellg();
				is.seekg(0, is.beg);
				// allocate memory:
				char *buffer = new char[length];

				// read data as a block:
				is.read(buffer, length);

				is.close();
				buff_str[i] = buffer;
				delete[] buffer;
			}
		}

		std::ofstream output;
		std::stringstream output_file;
		output_file << "output_" << output_count++ << ".txt";
		output.open(file_path + output_file.str());

		std::priority_queue<std::pair<std::string, int>, std::vector< std::pair<std::string, int>>, std::greater<std::pair<std::string, int>>> pq;
		std::string sentence;
		for (int i = 0; i < file_to_open;)
		{
			std::size_t found = buff_str[i].find('\n');
			if (found == std::string::npos)
				sentence = buff_str[i];
			else
				sentence = buff_str[i].substr(0, found);
			buff_str[i] = buff_str[i].substr(found + 1);
			if (sentence.find("2021") == std::string::npos)
				continue;
			if (sentence.size())
			{
				sentence = sentence.substr(0, 24) + files[i] + "," + sentence.substr(24);
				pq.push(make_pair(sentence, i));
			}

			i++;
		}

		std::string output_buffer = "";
		while (!pq.empty())
		{
			std::string output_sentence = pq.top().first;
			int index_popped = pq.top().second;
			pq.pop();

			//Dumping output_buffer to output memory file 
			output_buffer += output_sentence + '\n';
			if (output_buffer.size() >= ram_size / 5)
			{
				output << output_sentence;
				output_buffer.clear();
			}

			if (buff_str[index_popped].size())
			{
				std::string temp;
				std::size_t found = buff_str[index_popped].find('\n');
				if (found == std::string::npos)
					temp = buff_str[index_popped];
				else
					temp = buff_str[index_popped].substr(0, found);
				buff_str[index_popped] = buff_str[index_popped].substr(found + 1);
				if (temp.find("2021") != std::string::npos && temp.size())
				{
					temp = temp.substr(0, 24) + files[index_popped] + "," + temp.substr(24);
					pq.push(make_pair(temp, index_popped));
				}
			}
		}

		output.close();
		if (file_index >= files.size())
			break;
	}
}

/*! @brief Inefficient[worst case]  function to read from files and write the data to output 
//	Reading and writing raw symbol files in line by line.
*/
void initialMerge(const std::vector<std::string > &files, const std::string &file_path, int &ram_size)
{
		int curr_memory = 0;
		int file_index = 0;
		int output_count = 0;
		ram_size = static_cast<int> (0.9 *ram_size);
		while (1)
		{
			int file_to_open = 0;
			while (file_index < files.size() && curr_memory < ram_size)
			{
				curr_memory += getFileSize(file_path + files[file_index++]);
				file_to_open++;
			}

			std::vector<std::ifstream > inputs(file_to_open);
			for (int i = 0; i < file_to_open; i++)
			{
				inputs[i].open(file_path + files[i]);
			}

			std::ofstream output;
			std::stringstream output_file;
			output_file << "output_" << output_count++ << ".txt";

			output.open(file_path + output_file.str());
			std::priority_queue<std::pair<std::string, int>, std::vector< std::pair<std::string, int>>, std::greater<std::pair<std::string, int>>> pq;

			for (int i = 0; i < file_to_open;)
			{
				std::string sentence;
				if (!inputs[i].eof())
				{
					getline(inputs[i], sentence);
					if (sentence.find("2021") == std::string::npos)
						continue;
					sentence = sentence.substr(0, 24) + files[i] + "," + sentence.substr(24);
					pq.push(make_pair(sentence, i));
				}

				i++;
			}

			while (!pq.empty())
			{
				std::string sentence = pq.top().first;
				int index_popped = pq.top().second;
				pq.pop();
				//std::string temp;
				output << sentence << std::endl;
				if (!inputs[index_popped].eof())
				{
					int MAX_LENGTH = 4096;
					char *temp_char = new char[MAX_LENGTH];
					//iStream.getline(line, MAX_LENGTH) && strlen(line) > 0;
					inputs[index_popped].getline(temp_char, MAX_LENGTH);
					std::string temp = temp_char;
					//getline(inputs[index_popped],temp);
					if (temp.find("2021") == std::string::npos)
						continue;
					temp = temp.substr(0, 24) + files[index_popped] + "," + temp.substr(24);

					pq.push(make_pair(temp, index_popped));
				}
			}

			for (int i = 0; i < file_to_open; i++)
				inputs[i].close();

			output.close();
			if (file_index >= files.size())
				break;
		}
}
/*! @brief Enlists the run files  */
void enlistRunFiles(std::vector<std::string > &run_files, const std::string file_path)
{
	DIR * dir;
	struct dirent * diread;
	if ((dir = opendir(file_path.c_str())) != nullptr)
	{
		while ((diread = readdir(dir)) != nullptr)
		{
			std::string temp(diread->d_name);
			if (temp.find("dat") != std::string::npos)
				run_files.push_back(diread->d_name);
		}

		closedir(dir);
	}
	else
		perror("opendir");
	return;
}

/*! @brief returns the current offset for the memory mapped files */
int getsize(std::vector<int> index_offset)
{
	int sum = 0;
	for (auto &each: index_offset)
	{
		sum += each;
	}

	return sum;
}

/*! @brief From k sorted files fetch chunks of memory and dump it into the final output file: MKTDATA.dat */
void kwaymerge(const std::string &file_path, const long int &ram_size)
{
	std::vector<std::string > run_files;
	enlistRunFiles(run_files, file_path);
	std::vector < long int > runOffset;
	int partition_size = static_cast<int> (0.8 *ram_size / run_files.size());

	//Creating output file as memory mapping can not happen to empty files
	std::string output_file = file_path + "MKTDATA.dat";
	std::ofstream output_object;
	output_object.open(output_file.c_str());
	output_object << "Output file " << std::endl;
	output_object.close();

	int _output_filed = open(output_file.c_str(), O_CREAT | O_RDWR);

	if (_output_filed < 0)
	{
		std::cerr << " Error opening output file " << output_file << std::endl;
		abort();
	}

	struct stat sb_output;
	if (fstat(_output_filed, &sb_output) == -1)
	{
		std::cerr << "Error finding size " << std::endl;
	}

	std::size_t length_output = sb_output.st_size;

	char *output_buffer = nullptr;
	output_buffer = reinterpret_cast<char*> (mmap(NULL, getRoundUpSize(partition_size *run_files.size()), PROT_WRITE, MAP_SHARED, _output_filed, 0 u));

	if (output_buffer == MAP_FAILED)
	{
		std::cerr << "Error mapping output file " << output_file << std::endl;
	}

	/*! Opening run_files comprised from the raw symbol files *****/
	std::vector<char*> input_buffer(run_files.size());
	std::vector<std::string > input_buffer_str(run_files.size());
	std::vector<int> _input_filed(run_files.size());
	std::vector<std::size_t > input_getFileSize(run_files.size());
	std::vector<int> input_offset(run_files.size(), 0);
	for (int i = 0; i < run_files.size(); i++)
	{
		std::string input_file_name = file_path + run_files[i];
		_input_filed[i] = open(input_file_name.c_str(), O_RDONLY);

		if (_input_filed[i] < 0)
		{
			std::cerr << "Error opening input file " << input_file_name << std::endl;
		}

		struct stat sb_input;
		if (fstat(_input_filed[i], &sb_input) == -1)
			std::cerr << "Error finding size " << input_file_name << std::endl;
		input_getFileSize[i] = static_cast<std::size_t > (sb_input.st_size);
	}

	std::priority_queue<std::pair<std::string, int>, std::vector< std::pair<std::string, int>>, std::greater<std::pair<std::string, int>>> pq;
	std::string sentence;
	for (int i = 0; i < run_files.size(); i++)
	{
		input_buffer[i] = reinterpret_cast<char*> (mmap(NULL, partition_size, PROT_READ, MAP_PRIVATE, _input_filed[i], 0 u));
		input_buffer_str[i] = input_buffer[i];
		std::size_t found = input_buffer_str[i].find('\n');
		sentence = input_buffer_str[i].substr(0, found);
		input_buffer_str[i] = input_buffer_str[i].substr(found + 1);
		input_offset[i] = found + 1;
		pq.push(make_pair(sentence, i));

	}

	std::string output_buff = "";
	int output_buffer_size = 0;
	while (!pq.empty())
	{
		std::string output_sentence = pq.top().first;
		int index_popped = pq.top().second;
		pq.pop();

		output_buff += output_sentence + '\n';

		//Load the output buffer to the output file 
		if (output_buff.size() > partition_size)
		{
			//truncate the output file to fit the output buffer size 
			ftruncate(_output_filed, output_buffer_size + output_buff.size());
			strcpy(output_buffer + output_buffer_size, output_buff.c_str());
			output_buffer_size += output_buff.size() + 1;
			output_buff.clear();
		}

		if (input_buffer_str[index_popped].size())
		{
			std::string temp;
			std::size_t found = input_buffer_str[index_popped].find('\n');
			if (found == std::string::npos)
			{
				//clear the data and load another chunk;
				input_buffer[index_popped] = reinterpret_cast<char*> (mmap(NULL, partition_size, PROT_READ, MAP_PRIVATE, _input_filed[index_popped], getRoundUpSize(input_offset[index_popped])));
				input_buffer_str[index_popped] = input_buffer[index_popped];
				std::size_t found = input_buffer_str[index_popped].find('\n');
				temp = input_buffer_str[index_popped].substr(0, found);
				input_buffer_str[index_popped] = input_buffer_str[index_popped].substr(found + 1);
				input_offset[index_popped] += found + 1;
				pq.push(make_pair(temp, index_popped));

			}
			else
			{
				temp = input_buffer_str[index_popped].substr(0, found);
				input_buffer_str[index_popped] = input_buffer_str[index_popped].substr(found + 1);
				input_offset[index_popped] += found + 1;
				pq.push(make_pair(temp, index_popped));
			}
		}
	}

	//unmaping  input memory mapped files 
	for (int i = 0; i < run_files.size(); i++)
	{
		if (munmap(input_buffer[i], input_offset[i]) != 0)
		{
			std::cerr << "Error unmapping input file memory " << std::endl;
			abort();
		}

		if (close(_input_filed[i]) != 0)
			std::cerr << "Error closing the input file" << std::endl;

	}

	ftruncate(_output_filed, output_buffer_size + output_buff.size());
	strcpy(output_buffer + output_buffer_size, output_buff.c_str());
	output_buffer_size += output_buff.size();

	//writing to the disk 
	if (msync(output_buffer, output_buffer_size, MS_SYNC) != 0)
	{
		std::cerr << "Error unmapping output file memory" << std::endl;
		abort();
	}

	//unmapping the output memory mapped file 
	if (munmap(output_buffer, output_buffer_size) != 0)
	{
		std::cerr << "Error unmapping the output file " << std::endl;
		abort();
	}

	if (ftruncate(_output_filed, output_buffer_size) != 0)
		std::cerr << "Error truncating output file " << std::endl;
	if (close(_output_filed) != 0)
		std::cerr << "Error closing the output file" << std::endl;
	return;
}

int main()
{
	char _path[4096];
	if(getcwd(_path,sizeof(_path)) != NULL)
	{
		std::file_path = _path;
	}
	else
	{
		std::cerr << "Path was not found " << std::endl;
	}
	std::vector<std::string > files;
	getSymbolFileNames(files, file_path);
	sort(files.begin(), files.end());

	/*
    for (const auto &file: files)
        std::cout << file << std::endl;
	*/
	long int ram_size = static_cast< long int > (findRamSize());
	//int ram_size = 1073741824;
	//int ram_size = 62566;
	initMergeMemoryMap(files, file_path, ram_size);	//Reading and writing raw files via memory mapping
	//initialMergeBlock(files,file_path,ram_size);									//Reading and writing raw files in blocks 
	//initialMerge(files,file_path,ram_size);										//Reading and writing raw files line by line 
	kwaymerge(file_path, ram_size);													//K way merge from External merge sort to consolidate all into one output file 
	std::cout << "Entire process took a total of: " << float(clock() - begin_time) / CLOCKS_PER_SEC * 1000 << " msec." << std::endl;
	return EXIT_SUCCESS;
}