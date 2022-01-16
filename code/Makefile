CC = mpicc
CXX = mpicxx
CXXFLAGS = -std=c++20 -O3 -lm -pthread -I/home/pp21/pp21s38/hw4/code 
CFLAGS = -std=c++20 -O3 -lm -pthread

# hw4: Worker.cc Scheduler.cc hw4.cc
# 	$(CXX) $(CXXFLAGS) -o $@ hw4.cc Worker.cc Scheduler.cc

hw4: hw4.cc
	$(CXX) $(CXXFLAGS) -o $@ hw4.cc /home/pp21/pp21s38/hw4/code/Worker.cc /home/pp21/pp21s38/hw4/code/Scheduler.cc

.PHONY: clean
clean:
	-rm -f hw4
	-rm -r ./output ./inter
	mkdir ./output ./inter
