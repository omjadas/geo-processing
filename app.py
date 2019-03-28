import json
import sys
from jsonReader import JSONReader
from mpi4py import MPI
from collections import defaultdict, Counter
from functools import reduce

GRID = "melbGrid(1).json"


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    with open(GRID, "r", encoding="utf-8") as grid:
        grid_data = json.load(grid)

    file = JSONReader(sys.argv[1], rank, size)

    grids = defaultdict(int)

    for tweet in file:
        grid = get_grid(tweet, grid_data)
        if grid:
            grids[grid] += 1

    data = comm.gather(grids, root=0)
    if rank == 0:
        print(
            reduce(lambda x, y: x.update(y) or x, (Counter(d) for d in data)))

    file.close()


def get_grid(tweet, grids):
    for grid in grids["features"]:
        try:
            x = tweet["doc"]["coordinates"]["coordinates"][0]
            y = tweet["doc"]["coordinates"]["coordinates"][1]
            if x > grid["properties"]["xmin"] and \
                    x <= grid["properties"]["xmax"] and \
                    y > grid["properties"]["ymin"] and \
                    y <= grid["properties"]["ymax"]:
                return grid["properties"]["id"]
        except:
            pass
    return ""


if __name__ == "__main__":
    main()
