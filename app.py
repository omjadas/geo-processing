import json
import re
import sys
from jsonReader import JSONReader
from mpi4py import MPI
from collections import defaultdict, Counter
from functools import reduce
from itertools import takewhile

GRID = sys.argv[2]


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
            for hashtag in get_hashtags(tweet["doc"]["text"]):
                hashtags[grid][hashtag.lower()] += 1

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
            print("{}: {}".format(grid[0], most_common(
                new_dict[grid[0]], 5)).encode("utf-8"))

    file.close()


def most_common(counter, n):
    data = counter.most_common()
    counts = set(hashtag[1] for hashtag in counter.most_common())
    try:
        val = sorted(counts, reverse = True)[n - 1]
    except IndexError:
        val = sorted(counts, reverse = True)[-1]
    return list(takewhile(lambda x: x[1] >= val, data))


def get_hashtags(tweet):
    return re.findall(" #([^ ]+) ", tweet)


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
    for grid in grids["features"]:
        try:
            x = tweet["doc"]["coordinates"]["coordinates"][0]
            y = tweet["doc"]["coordinates"]["coordinates"][1]
            if x >= grid["properties"]["xmin"] and \
                    x <= grid["properties"]["xmax"] and \
                    y >= grid["properties"]["ymin"] and \
                    y <= grid["properties"]["ymax"]:
                return grid["properties"]["id"]
        except BaseException:
            pass
    return ""


if __name__ == "__main__":
    main()
