import os
# from . import create_bazar #.create_bazar as create_bazar
from create_bazar import get_hop_count, create_bazar
from peers import Peer

"""
Test case 1
Description: There are 5 peers in the network. 4 buyers and 1 seller. 
All buyers request for salt and the seller is of salt. The seller initially has 2 salt items to sell. 
The buyers start a lookup, and matche with single seller. 

But randomly selects any seller.
That seller sells 1 salt item to the buyer and its total count decreases.

Result: A seller sends reply back to all the buyers it matched with. But among the matched buyers, only 1 buyer is locking the seller. 
And if the seller's items are 0, the buyer fails to make a purchase. 
"""



ids = {
    "buyer0": "buyer",
    "buyer1": "buyer",
    "buyer2": "buyer",
    "buyer3": "buyer",
    "seller4": "seller",

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
    
