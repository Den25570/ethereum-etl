
import json

def readJson(path):
    infile = None
    try:
        infile = open(path, 'r+')
        data = json.load(infile)
    except Exception:
        data = None
    finally:
        if infile != None:
            infile.close()
    return data

def writeJson(path, data):
    outfile = None
    try:
        outfile = open(path, 'w')
        json.dump(data, outfile, indent=4)
    finally:
        if outfile != None:
            outfile.close()