#!/usr/bin/python

# import
import sys
import os
from importlib import import_module
from settings import conf

def main():
    # Gives user's home directory (a way to take the user name)
    userhome = os.path.expanduser('~')
    user_name = os.path.split(userhome)[-1]

    # Get the name of the class from arguments
    #class_name = sys.argv[1]
    class_name = "DT_CUSTOMER"

    # Dynamically load the job class
    cls = getattr(import_module('jobs.' + class_name), class_name)

    # Create a job object and run
    job = cls(conf, args = sys.argv[2:], user_name = user_name)
    job.run()

if __name__ == "__main__":
    main()
