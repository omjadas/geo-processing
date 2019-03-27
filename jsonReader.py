import json


class JSONReader():
    def __init__(self, fname, offset=0, nodes=1):
        self.f = open(fname, "r", encoding="utf-8")
        self.offset = offset
        self.nodes = nodes
        self.first = True
        next(self.f)

    def __iter__(self):
        return self

    def __next__(self):
        if self.first:
            for i in range(self.nodes - self.offset):
                line = next(self.f)
            self.first = False
        else:
            for i in range(self.nodes):
                line = next(self.f)
        if line[-2:] == ",\n":
            line = line[:-2]
        if line == "]}\n":
            raise StopIteration
        else:
            return json.loads(line)

    def close(self):
        self.f.close()
