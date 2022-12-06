import json
from logging import exception
import os
import sys
import random
import sys
from peers import Peer
from create_bazar import create_bazar
from database import DbHandler
# It is responsible for spwaning peers as buyers or sellers

def main():
    config_file = open(os.path.join(os.path.dirname(__file__), '../..', 'config.json'))
    data = json.load(config_file)
    default_configs = data["default_config"]
    # If number of peers is passed through command line then use that as number of peer
    # otherwise pick default number of peers from config file
    try:
        hostname = sys.argv[1]
    except Exception as e:
        print("Host name not provided please provide a hostname while executing the run command")
    if len(sys.argv) >= 3:
        number_of_peers = int(sys.argv[2])
    else:
        number_of_peers = default_configs["number_of_peers"]
    
    if len(sys.argv) >= 4:
        number_of_traders = min(3, int(sys.argv[3]))
    else:
        number_of_traders = default_configs["number_of_peers"]

    roles = default_configs["roles"]
    items = default_configs["items"]
    items_count = default_configs["number_of_items"]
    peers = []
    # Make sure that there is at least one buyer and one seller in the network
    ids = {}
    for i,r in enumerate(roles):
        id = f"{r}{str(i)}"
        ids[id] = r
    base_path = os.getcwd()

    for i in range(2,number_of_peers):
        role = roles[random.randint(0,len(roles)-1)]
        id = f"{role}{str(i)}"
        ids[id] = role
    for id,role in ids.items():
        peer = Peer(id,role,items,items_count,hostname,base_path, 2)
        peers.append(peer)
    return peers

        

if __name__=='__main__':
    peers = main()
    peer_id_list = []
    for peer in peers:
        peer_id_list.append(peer.id)

    for peer in peers:
        for peer_id in peer_id_list:
            if peer_id == peer.id:
                continue
            peer.neighbors.append(peer_id)
            if peer.role == "seller":
                item_cost = random.randint(5,10)
                peer.price = item_cost
    edges = create_bazar(peer_id_list, True)
    base_path = os.getcwd()
    # Check database connection status

    database_client = DbHandler()
    try:
        if database_client.test_connection():
            database_client.reset_database()
            print("Connected succesfuly to the databse")
        else:
            print("Something went wrong while connecting to database.... Exiting!!!")
            sys.exit()
    except Exception as e:
        print(f"Database connection failed with error {e}. Application will now exit!!!")
    new_path = f"{base_path}/database_process"
    isExist = os.path.exists(new_path)
    if not isExist:
        os.makedirs(new_path)
    os.chdir(new_path)
    database_client.start()
    try: 
        for i,peer in enumerate(peers):
            new_path = f"{base_path}/peer/peer{i}"
            isExist = os.path.exists(new_path)
            if not isExist:
                os.makedirs(new_path)
            os.chdir(new_path)
            peer.start()
    except KeyboardInterrupt:
        sys.exit()
    
