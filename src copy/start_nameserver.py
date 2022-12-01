import Pyro4.naming
import Pyro4
import sys
from multiprocessing import Process

# This function starts a Pyro5 nameserver
# All pyro objects will be registered to this nameserver
import os
from multiprocessing import Process


def detachify(func):
    #Decorate a function so that its calls are async in a detached process
    # This will prevent terminal from getting blocked and hence output from any further execution will be
    # visible on terminal
    # create a process fork and run the function
    def forkify(*args, **kwargs):
        if os.fork() != 0:
            return
        func(*args, **kwargs)

    # wrapper to run the forkified function
    def wrapper(*args, **kwargs):
        proc = Process(target=lambda: forkify(*args, **kwargs))
        proc.start()
        proc.join()
        return

    return wrapper

@detachify
def detached_nameserver(host):
        Pyro4.naming.startNSloop(host=host)

def start_nameserver():
    arguments = sys.argv
    host = ""
    try:
        host = arguments[1]
    except Exception as e:
        print("Host not provided for namespace server to start!!!")

    try:
        # Check if the nameserver is already running
        Pyro4.locateNS(host=host)
        print(f"Namespace server is already running on {host}")
    except Exception as e:
        print("No nameserver found!!! Starting a new server on the host machine")
        try:
            # Starting a namespace server as a seperate process in detached mode
            detached_nameserver(host)
            
        except Exception as e:
            print(f"Starting the nameserver failed with exception {e}")

if __name__=='__main__':
    start_nameserver()


