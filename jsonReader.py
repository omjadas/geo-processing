import json


class JSONReader():
    def __init__(self, fname):
        self.f = open(fname, "r", encoding="utf-8")
        next(self.f)

    def __next__(self):
        return json.loads(next(self.f)[:-2])

    def close(self):
        self.f.close()
