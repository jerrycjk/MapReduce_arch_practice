#include <iostream>
#include <string>
#include <fstream>
#include <algorithm>
#include <vector>
#include <mpi.h>
#include "Scheduler.h"
#include "Worker.h"

bool Check_correctness(int num_reducer, std::string job_name, std::string output_dir) {
    std::ifstream result_file ;
    std::vector<std::pair<std::string, int>> total_results ;
    std::string word, pre_word ;
    int counts ;

    for (int i=0; i<num_reducer; i++) {
        pre_word = "0" ;
        result_file.open(output_dir + job_name + "-" + std::to_string(i+1) + ".out") ;
        while (result_file >> word >> counts) {
            if (word[0] < pre_word[0]) {
                result_file.close() ;
                std::cout << "In some file, result is not sorted \n" ;
                return false ;
            }
            total_results.emplace_back(word, counts) ;
            pre_word = word ;
        }

        result_file.close() ;
    }
    std::sort(total_results.begin(), total_results.end(), compare_func) ;

    std::vector<std::pair<std::string, int>> ans ;
    result_file.open("../testcases/" + job_name.substr(job_name.find("0", 0), job_name.npos) + ".ans") ;
    while (result_file >> word >> counts) {
        ans.emplace_back(word, counts) ;
    }
    result_file.close() ;

    std::ofstream total_file ;
    total_file.open(output_dir + "total_results.txt") ;
    for (auto r : total_results) {
        total_file << r.first << " " << r.second << "\n" ;
    }
    total_file.close() ;

    return total_results == ans ;
}

int main(int argc, char *argv[]) {
    int provided_level ;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided_level) ;
    // args
    std::string job_name = std::string(argv[1]);
    int num_reducer = std::stoi(argv[2]);
    int delay = std::stoi(argv[3]);
    std::string input_filename = std::string(argv[4]);
    int chunk_size = std::stoi(argv[5]);
    std::string locality_config_filename = std::string(argv[6]);
    std::string output_dir = std::string(argv[7]);
    // rank, size
    int rank, size ;
    MPI_Comm_size(MPI_COMM_WORLD, &size) ;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank) ;

    if (rank == 0) { // job tracker
        Scheduler scheduler(size, job_name, num_reducer, delay, input_filename, chunk_size, locality_config_filename, output_dir) ;
        scheduler.Map_phase() ;
        std::cout << "Scheculer finish map phase\n" ;
        scheduler.Reduce_phase() ;
        std::cout << "Scheculer finish reduce phase\n" ;
        MPI_Barrier(MPI_COMM_WORLD) ;

        // if (Check_correctness(num_reducer, job_name, output_dir)) {
        //     std::cout << "Correct\n" ;
        // }
        // else std::cout << "Something wrong\n" ;
    }
    else { // worker
        Worker worker(size, rank, job_name, num_reducer, delay, input_filename, chunk_size, output_dir) ;
        worker.Map_phase() ;
        std::cout << "Mapper " << rank << " finish map phase\n" ;
        worker.Reduce_phase() ;
        std::cout << "Mapper " << rank << " finish reduce phase\n" ;
        MPI_Barrier(MPI_COMM_WORLD) ;
    }

    MPI_Finalize() ;

    return 0 ;
}