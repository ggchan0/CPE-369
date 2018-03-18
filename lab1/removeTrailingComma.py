import sys

print sys.argv

if len(sys.argv) != 2:
    exit()

f = open(sys.argv[1], "r")

for line in f:
    print line.rstrip().rstrip(",")

