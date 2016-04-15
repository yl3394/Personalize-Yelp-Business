#!/usr/bin/env python

# run as python script

import sys
import argparse

# the raw args from the cmd line
print('raw args:', sys.argv)


parser = argparse.ArgumentParser()

# required positional arg
parser.add_argument("intarg1", type=int,
                    help="must be an integer")

# 2nd required positional arg
parser.add_argument("arg2", type=str,
                    help="arg is a string")

# optional '-' flag with no arg
parser.add_argument("-v", "--verbose", help="increase output verbosity",
                    action="store_true")


# optional '-' flag with required arg
parser.add_argument("-e", "--exit", help="increase output verbosity",
			 type=int)


# parsed args - will automatically print errors and abort on bad args
args = parser.parse_args()
print(args)

print('intarg1=', args.intarg1)
print('arg2=', args.arg2)


# args.verbose will = None if no arg
if args.verbose:
    print("verbosity turned on")

if args.exit:
   print("exit with:", args.exit)
   # in bash, print with:  echo $?
   sys.exit(args.exit)
   print("won't get here")


print(sys.argv[0])