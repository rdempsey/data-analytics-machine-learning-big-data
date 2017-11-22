#!/usr/bin/env python


import json
import sys

"""
Formats a JSON file into a Spark SQL compatible JSON format.

Takes 2 arguments, path to input file and path to desired output file.
"""
if __name__ == '__main__':
    if len(sys.argv) != 3:
        print "Error usage: clean_json [inputfile] [outputfile]"
        sys.exit(-1)
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    raw_json = open(input_file)
    data = json.load(raw_json)
    clean_json = open(output_file, 'w')
    count = 0

    for row in data:
        clean_json.write(json.dumps(row))
        clean_json.write('\n')
        count += 1

    print "Successfully wrote %d records to %s" % (count, output_file)
