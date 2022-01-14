#include <pthread.h>
#include <string>
#include <vector>

class Worker
{
private:
    int thread_num ;

    pthread_t *threads ;
public:
    Worker(int rank, std::string job_name, int num_reducer, int delay, std::string input_filename, int chunk_size, std::string output_dir);
    ~Worker();

    int rank, num_reducer, delay, chunk_size ;
    std::string job_name ;
    std::string output_dir ;
    std::string input_filename ;

    int *task ; // task[0]: task_chunkIdx, task[1]: task_used
    bool done ;
    int waiting_threads ;
    pthread_mutex_t work_lock, write_lock ;
    pthread_cond_t cond ;

    std::vector<std::pair<std::string, int>> *inter_results ;

    void Map_phase() ;

    void Map_functions(int chunkIdx) ;
    std::vector<std::pair<int, std::string>> Input_split(int chunkIdx) ;
    std::vector<std::pair<std::string, int>> Map(std::vector<std::pair<int, std::string>> record) ;
    void Partition(std::vector<std::pair<std::string, int>> out_record) ;

    void Shuffle() ;
};

