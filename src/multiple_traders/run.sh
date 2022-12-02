#!/bin/sh
echo "+++++++++++++Starting The Bazar+++++++++++++++++++++++"
# Install all the requirements
echo "Installing all the requirements for the project"
pip3 install -r  requirements.txt
echo "Starting nameserver in detached mode"
python3 start_nameserver.py "$@"
echo "Removing dead peers from the server"
echo y | pyro4-nsc removematching ^seller
echo "Removing dead peers from the server"
echo y | pyro4-nsc removematching ^buyer
echo "Starting the main process"
python3 main.py "$@"


