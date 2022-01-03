#include <mpi.h>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <fstream>
#include <atomic>
#include <functional>
#include <condition_variable>
#include <unistd.h>
#include <sstream>
#include <cctype>
#include <stdlib.h>

#define SYSTEM_THREADS	(sysconf(_SC_NPROCESSORS_CONF))

#define RANK_MASTER				(0)
#define RANK_HORROR				(1)
#define RANK_COMEDY				(2)
#define RANK_FANTASY			(3)
#define RANK_SCIENCE_FICTION	(4)

#define ID_HORROR			("horror")
#define ID_COMEDY			("comedy")
#define ID_FANTASY			("fantasy")
#define ID_SCIENCE_FICTION	("science-fiction")

#define BUFFER_SIZE	(1000)
#define CHUNK_SIZE	(20)

#define CONSONANTS (std::string("BCDFGHJKLMNPQRSTVWXYZ"))

int id_to_rank(std::string& id) {
	static std::map<std::string, int> id_to_rank_map = {
		{ ID_HORROR, 			RANK_HORROR },
		{ ID_COMEDY, 			RANK_COMEDY },
		{ ID_FANTASY, 			RANK_FANTASY },
		{ ID_SCIENCE_FICTION, 	RANK_SCIENCE_FICTION }
	};

	return id_to_rank_map[id];
}

std::string rank_to_id(int rank) {
	static std::map<int, std::string> rank_to_id_map = {
		{ RANK_HORROR, 			ID_HORROR },
		{ RANK_COMEDY, 			ID_COMEDY },
		{ RANK_FANTASY, 		ID_FANTASY },
		{ RANK_SCIENCE_FICTION, ID_SCIENCE_FICTION }
	};

	return rank_to_id_map[rank];
}

void master_thread(int worker_rank, std::queue<std::string>& thread_results,
	std::map<int, int>& par_assignation, std::mutex& par_assignation_mutex,
	std::string file_name) {

	std::ifstream in_file(file_name);
	std::string curr_line;

	enum State { RESET, READING, SKIPPING };
	State curr_state = RESET;
	int paragraph_index = 0;

	std::string own_id = rank_to_id(worker_rank);
	std::string paragraph = "";

	std::queue<int> processed_par_indexes;
	bool stop = false;

	while (std::getline(in_file, curr_line) || in_file.eof()) {
		switch (curr_state) {
		case RESET:
		{
			if (in_file.eof())
				stop = true;
			else if (curr_line == own_id)
				curr_state = READING;
			else
				curr_state = SKIPPING;

			break;
		}
		case READING:
		{
			// build up the paragraph
			paragraph += curr_line + "\n";

			if (curr_line.empty() || in_file.eof()) {
				// finished reading a paragraph, send to worker
				MPI_Send(paragraph.c_str(), paragraph.size(),
					MPI_CHAR, worker_rank, 0, MPI_COMM_WORLD);

				// wait for the worker to process the paragraph
				MPI_Status status;
				MPI_Probe(worker_rank, 0, MPI_COMM_WORLD, &status);

				int message_size;
				MPI_Get_count(&status, MPI_CHAR, &message_size);

				char processed_par[message_size + 1] = { 0 };

				MPI_Recv(processed_par, message_size + 1, MPI_CHAR, worker_rank, 0,
					MPI_COMM_WORLD, &status);

				// save the result
				std::string result_str(processed_par);
				thread_results.push(result_str);

				// mark that this paragraph has been processed by this worker
				par_assignation_mutex.lock();
				par_assignation[paragraph_index++] = worker_rank;
				par_assignation_mutex.unlock();

				// reset for future uses
				paragraph = "";
				curr_state = RESET;
			}

			break;
		}
		case SKIPPING:
		{
			if (curr_line.empty() || in_file.eof()) {
				paragraph_index++;
				curr_state = RESET;
			}

			break;
		}
		default:
			break;
		}

		if (stop)
			break;
	}

	in_file.close();

	// tell the worker to stop execution by sending a tag 1 message
	MPI_Send("", 0, MPI_CHAR, worker_rank, 1, MPI_COMM_WORLD);
}

void master_main(std::string file_name) {
	std::map<int, int> par_assignation;
	std::mutex par_assignation_mutex;

	std::vector<std::thread> threads;
	std::vector<std::queue<std::string>*> results;

	// the 4 workers
	for (int rank = 1; rank <= 4; rank++) {
		std::queue<std::string>* thread_results = new std::queue<std::string>();
		results.push_back(thread_results);

		std::thread new_thread = std::thread(
			[rank, thread_results, &par_assignation,
			&par_assignation_mutex, &file_name] {
				master_thread(rank, *thread_results,
					par_assignation, par_assignation_mutex,
					file_name);
			}

		);
		threads.push_back(move(new_thread));
	}

	// wait for all the workers to finish processing
	for (std::thread& thread : threads)
		if (thread.joinable())
			thread.join();

	// begin writing to file
	int extension_start = file_name.find_last_of('.');
	std::string out_file_name = file_name.substr(0, extension_start) + ".out";
	std::ofstream out_file(out_file_name);

	for (int par_index = 0; par_index < par_assignation.size(); par_index++) {
		int assigned_thread = par_assignation[par_index];
		std::queue<std::string>* thread_results = results[assigned_thread - 1];

		std::string curr_result = thread_results->front();
		thread_results->pop();

		out_file << rank_to_id(assigned_thread) << '\n' << curr_result;
	}

	out_file.close();

}

std::string worker_horror(std::string line, int thread_id) {
	std::string new_line;

	for (int i = 0; i < line.size(); i++) {
		new_line += line[i];

		if (CONSONANTS.find(toupper(line[i])) != std::string::npos)
			new_line += tolower(line[i]);
	}

	return new_line;
}

std::string worker_comedy(std::string line, int thread_id) {
	std::string new_line;

	int word_index = 0;
	for (int i = 0; i < line.size(); i++) {
		char char_to_add = line[i];

		if (isspace(char_to_add))
			word_index = 0;
		else
			word_index++;

		if (word_index > 0 && word_index % 2 == 0)
			char_to_add = toupper(char_to_add);

		new_line += char_to_add;
	}

	return new_line;
}

std::string worker_fantasy(std::string line, int thread_id) {
	std::string new_line;

	int word_index = 0;
	for (int i = 0; i < line.size(); i++) {
		char char_to_add = line[i];

		if (isspace(char_to_add))
			word_index = 0;
		else
			word_index++;

		if (word_index == 1)
			char_to_add = toupper(char_to_add);

		new_line += char_to_add;
	}

	return new_line;
}

std::string worker_science_fiction(std::string line, int thread_id) {
	std::string new_line;

	int word_count = 0;
	bool reading_word = false;
	std::string curr_word;

	for (int i = 0; i < line.size(); i++) {
		char curr_char = line[i];

		if (isalpha(curr_char)) {
			if (!reading_word) {
				reading_word = true;
				word_count++;
			}

			curr_word += curr_char;
		} else {
			if (reading_word) {
				reading_word = false;

				if (word_count % 7 == 0)
					curr_word = std::string(
						curr_word.rbegin(), curr_word.rend()
					);

				new_line += curr_word;
				curr_word = "";
			}

			new_line += curr_char;
		}

	}

	// while (std::getline(ss, curr_word, ' ')) {
	// 	std::string processed_word = curr_word;

	// 	if (++word_count % 7 == 0)
	// 		processed_word = std::string(curr_word.rbegin(), curr_word.rend());

	// 	new_line += processed_word + " ";
	// }

	return new_line;
}

void worker_main(int rank) {
	while (true) {
		MPI_Status status;
		MPI_Probe(RANK_MASTER, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

		int message_size;
		MPI_Get_count(&status, MPI_CHAR, &message_size);

		char paragraph[message_size + 1] = { 0 };

		MPI_Recv(paragraph, message_size + 1, MPI_CHAR, RANK_MASTER,
			MPI_ANY_TAG, MPI_COMM_WORLD, &status);

		// stop
		if (status.MPI_TAG == 1)
			break;

		// break into lines
		std::string par_string(paragraph);
		std::string curr_line;
		std::stringstream ss(par_string);

		std::vector<std::string> lines;

		while (std::getline(ss, curr_line))
			lines.push_back(curr_line + "\n");

		std::vector<std::thread> threads;

		for (int thread_id = 0; thread_id < SYSTEM_THREADS - 1; thread_id++) {
			// add a new task to be executed by the thread pool
			std::function<void()> func = [&lines, thread_id, rank] {
				int first_chunk = thread_id * CHUNK_SIZE;

				for (int start = first_chunk; start < lines.size(); start += CHUNK_SIZE) {
					for (int offset = 0; offset < CHUNK_SIZE; offset++) {
						int line_index = start + offset;
						if (line_index >= lines.size())
							return;

						std::string line = lines[line_index];
						std::string new_line;

						switch (rank) {
						case RANK_HORROR:
							new_line = worker_horror(line, thread_id);
							break;
						case RANK_COMEDY:
							new_line = worker_comedy(line, thread_id);
							break;
						case RANK_FANTASY:
							new_line = worker_fantasy(line, thread_id);
							break;
						case RANK_SCIENCE_FICTION:
							new_line = worker_science_fiction(line, thread_id);
							break;
						default:
							break;
						}

						lines[line_index] = new_line;
					}
				}
			};

			std::thread new_thread(func);
			threads.push_back(move(new_thread));
		}

		for (std::thread& thread : threads)
			if (thread.joinable())
				thread.join();

		// rebuild the result
		std::string rebuilt_paragraph;
		for (std::string line : lines)
			rebuilt_paragraph += line;

		// send the processed paragraph back to the master node
		MPI_Send(rebuilt_paragraph.c_str(), rebuilt_paragraph.size(),
			MPI_CHAR, RANK_MASTER, 0, MPI_COMM_WORLD);
	}
}

int main(int argc, char* argv[]) {
	int rank, provided;

	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	switch (rank) {
	case RANK_MASTER:
		master_main(argv[1]);
		break;

	default:
		worker_main(rank);
		break;
	}

	MPI_Finalize();
}