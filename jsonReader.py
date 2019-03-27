import json


class JSONReader():
    def __init__(self, fname):
        self.f = open(fname, "r", encoding="utf-8")
        next(self.f)

    def __iter__(self):
        return self

    def __next__(self):
        line = next(self.f)
        if line[-2] == ",":
            line = line[:-2]
        if line == "]}\n":
            raise StopIteration
        else:
            return json.loads(line)

    def close(self):
        self.f.close()
