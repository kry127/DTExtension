import pathlib

if __name__ == "__main__":
    p = pathlib.PurePath('c:/foo/bar/setup.py')
    print([p for p in p.parents])
