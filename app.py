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
    hashtags = defaultdict(lambda: defaultdict(int))

    for tweet in file:
        grid = get_grid(tweet, grid_data)
        if grid:
            grids[grid] += 1
            for hashtag in tweet["doc"]["entities"]["hashtags"]:
                hashtags[grid][hashtag["text"].lower()] += 1

    grids = dict(grids)
    hashtags = dict(hashtags)

    grid_data = comm.gather(grids, root=0)
    hashtag_data = comm.gather(hashtags, root=0)
    if rank == 0:
        total_tweets = reduce(
            lambda x, y: x.update(y) or x,
            (Counter(d) for d in grid_data))

        for grid in total_tweets.most_common():
            print("{}: {} posts".format(grid[0], grid[1]))

        new_dict = defaultdict(lambda: Counter())
        for h in hashtag_data:
            for grid in h:
                for hashtag in h[grid]:
                    new_dict[grid][hashtag] += h[grid][hashtag]
        for grid in total_tweets.most_common():
            print("{}: {}".format(grid[0], new_dict[grid[0]].most_common(5)))
        # print("{}".format(reduce(lambda x, y: x.update(y) or x, {h: Counter(hashtags[h]) for h in hashtags})).encode("utf-8"))

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
        except BaseException:
            pass
    return ""


if __name__ == "__main__":
    main()
