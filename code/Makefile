CC = mpicc
CXX = mpicxx
CXXFLAGS = -std=c++20 -O3 -lm -pthread
CFLAGS = -std=c++20 -O3 -lm -pthread

hw4: main.cc Worker.cc Scheduler.cc
	$(CXX) $(CXXFLAGS) main.cc Worker.cc Scheduler.cc -o $@

.PHONY: clean
clean:
	-rm -f hw4
	-rm -r ./output ./inter
	mkdir ./output ./inter
