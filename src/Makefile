CPPFLAGS=-O3 -ffast-math
#CPPFLAGS=-g 
LDFLAGS=-lpthread $(CPPFLAGS)
all: main

CC=g++

allmodels.o: allmodels.cpp allmodels.h data.h
main.o: main.cpp data.h benchmark.h
data.o: data.cpp data.h
benchmark.o: benchmark.cpp benchmark.h

main: main.o data.o benchmark.o allmodels.o
#$(CXX) $(CPPFLAGS) -o $@ $^
svd.so: svd.pyx
	python setup.py build_ext -i

clean:
	rm -f svd.so svd.c main *.o
