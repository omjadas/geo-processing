import json


class JSONReader():
    def __init__(self, fname, offset=0, processes=1):
        self.f = open(fname, "r", encoding="utf-8")
        self.offset = offset
        self.processes = processes
        self.first = True

        # Skip the first line in the file
        next(self.f)

    def __iter__(self):
        return self

    def __next__(self):
        # For the first tweet obtained from the file, the offset needs to be
        # taken into account, so that each process reads different tweets
        if self.first:
            for i in range(self.processes - self.offset):
                line = next(self.f)
            self.first = False
        else:
            # Iterate through the file once for each process
            for i in range(self.processes):
                line = next(self.f)
        if line[-2:] == ",\n":
            # Slice the end comma and newline from the line
            line = line[:-2]
        if line == "]}\n":
            # Raise StopIteration if the file has been completely read
            raise StopIteration
        else:
            # Parse the string and return
            return json.loads(line)

    def close(self):
        """Close the file"""
        self.f.close()
