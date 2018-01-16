import sys
from random import *

filename = sys.argv[1]

f = open(filename, "r")

for line in f:
    firstNum = str(randint(1, 100))
    secondNum = str(randint(1, 1000))
    print line.strip() + ", " + firstNum + ", " + secondNum


