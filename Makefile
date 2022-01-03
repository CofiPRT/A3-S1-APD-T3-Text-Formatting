default: build

build:
	mpic++ main.cpp -o main -O3

run: build
	mpirun -oversubscribe -np 5 main