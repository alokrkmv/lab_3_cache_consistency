import os
# from . import create_bazar #.create_bazar as create_bazar
from create_bazar import get_hop_count, create_bazar
from peers import Peer

"""
Test case 1
Description: There are 2 peers in the network. 1 buyer and 1 seller. 
Buyer requests for salt and the seller is of salt. The seller initially has 5 salt items to sell. 
The buyr starts a lookup, and matches with the seller. The seller sells 1 salt item to the buyer and its total count decreases.
The buyer then selects another item for buying, but doesnnt match with the buyer untill it selects salt again. 
When buyer selects salt again it matches with the seller and buys salt. This continues till seller's salt stock is finished.

Result: The test shows that buyer can only buy the item which the seller has, it keeps changing the item it wants to buy randomly
and keeps buying the item salt until sellers items get finished.
"""



ids = {
    "buyer0": "buyer",
    "seller1": "seller"
}

items = ["fish","salt","boar"]
items_count = 5
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
    print(peer.role, peer.item)

for peer in peers:
    peer.start()
    
