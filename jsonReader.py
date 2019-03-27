import json


class JSONReader():
    def __init__(self, fname, offset=0, nodes=1):
        self.f = open(fname, "r", encoding="utf-8")
        self.offset = offset
        self.nodes = nodes
        next(self.f)
        for i in range(offset):
            next(self.f)

    def __iter__(self):
        return self

    def __next__(self):
        for i in range(self.nodes):
            line = next(self.f)
        if line[-2] == ",":
            line = line[:-2]
        if line == "]}\n":
            raise StopIteration
        else:
            return json.loads(line)

    def close(self):
        self.f.close()
