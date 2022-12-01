import os
# from . import create_bazar #.create_bazar as create_bazar
from create_bazar import get_hop_count, create_bazar
from peers import Peer

"""
Test case 1
Description: There are 2 peers in the network. 1 buyer and 1 seller. 
Buyer requests for salt and the seller is of salt. The seller initially has 2 salt items to sell. 
The buyer starts a lookup, and matches with the seller. The seller sells 1 salt item to the buyer and its total count decreases.
When the total count of items of the seller goes to 0, it randomly pick another item and not the same item to sell.

Result: Every time seller's items finish, it picks another item to sell.
"""



ids = {
    "buyer0": "buyer",
    "seller1": "seller"
}

items = ["fish","salt","boar"]
items_count = 2
hostname = "localhost"
base_path = os.getcwd()
peers = []
for id, role in ids.items():
    peer = Peer(id, role, items, items_count, hostname, base_path)
    peers.append(peer)

# print(peers)
peer_id_list = []
for peer in peers:
    peer_id_list.append(peer.id)
edges = create_bazar(peer_id_list)
hop_count = get_hop_count(edges)

for peer in peers:
    peer.hop_count = 2

    if peer.role == "buyer":
        peer.item = ["fish","salt","boar"][1]

    if peer.role == "seller":
        peer.item = ["fish","salt","boar"][1]
        peer.max_items = 2

    print(peer.role, peer.item)

for peer in peers:
    peer.start()
    
