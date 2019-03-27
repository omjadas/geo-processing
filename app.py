import json
import sys
from jsonReader import JSONReader
from mpi4py import MPI


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    file = JSONReader(sys.argv[1], rank, size)

    file.close()


if __name__ == "__main__":
    main()
