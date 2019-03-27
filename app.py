import json
import sys
from jsonReader import JSONReader


def main():
    file = JSONReader("tinyTwitter(3).json")
    file.close()


if __name__ == "__main__":
    main()
