import os


def makedir(path):
    try:
        os.makedirs(path)
    except FileExistsError:
        # directory already exists
        pass
