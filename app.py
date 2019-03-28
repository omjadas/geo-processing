import json
import sys
from jsonReader import JSONReader
from mpi4py import MPI
from collections import defaultdict

GRID = "melbGrid(1).json"


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    with open(GRID, "r", encoding="utf-8") as grid:
        grid_data = json.load(grid)

    file = JSONReader(sys.argv[1], rank, size)

    d = defaultdict(int)

    for tweet in file:
        grid = get_grid(tweet, grid_data)
        if grid:
            d[grid] += 1

    if rank == 0:
        pass
    else:
        pass

    file.close()


def get_grid(tweet, grids):
    for grid in grids["features"]:
        x = tweet["value"]["geometry"]["coordinates"][0]
        y = tweet["value"]["geometry"]["coordinates"][1]
        if x > grid["properties"]["xmin"] and x <= grid["properties"][
                "xmax"] and y > grid["properties"]["ymin"] and y <= grid["properties"]["ymax"]:
            return grid["properties"]["id"]
    return ""


if __name__ == "__main__":
    main()
