import json
import sys
from jsonReader import JSONReader
from mpi4py import MPI


def main():
    file = JSONReader("tinyTwitter(3).json")
    file.close()


if __name__ == "__main__":
    main()
