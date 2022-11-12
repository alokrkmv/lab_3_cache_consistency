from create_bazar import create_bazar, get_longest_path, get_hop_count
import unittest
import os
from peers import Peer
# nameserver
# create bazar is connected
# create bazaar graph getting created
# checks minimum number of buyrs and sellers


class TestCreateBazar(unittest.TestCase):

    def test_get_longest_path(self):
        d = [
            [0, 1, 0, 0, 1],
            [1, 0, 1, 0, 0],
            [0, 1, 0, 1, 0],
            [0, 0, 1, 0, 1],
            [1, 0, 0, 1, 0],
        ]
        assert get_longest_path(d) == 4

    def test_get_longest_path2(self):


        d = [
            [0, 1, 0, 0, 0, 1],
            [1, 0, 1, 0, 0, 0],
            [0, 1, 0, 1, 1, 0],
            [0, 0, 1, 0, 1, 0],
            [0, 0, 1, 1, 0, 1],
            [1, 0, 0, 0, 1, 0]
        ]

        assert get_longest_path(d) == 5

    def test_get_longest_path2(self):

        d = [
        [0, 1, 0, 0, 0, 0, 0, 0, 0, 0],
        [1, 0, 1, 0, 0, 0, 0, 0, 0, 0],
        [0, 1, 0, 1, 0, 0, 0, 0, 0, 0],
        [0, 0, 1, 0, 1, 1, 0, 0, 0, 0],
        [0, 0, 0, 1, 0, 1, 1, 0, 0, 0],
        [0, 0, 0 ,0 ,1, 0, 1, 1, 0, 0],
        [0, 0, 0 ,0 ,1, 1, 0, 1, 0, 0],
        [0, 0, 0 ,0, 0, 1, 1, 0, 1, 0],
        [0, 0, 0 ,0, 0, 0, 0, 1, 0, 1],
        [0, 0, 0 ,0, 0, 0, 0, 0, 1, 0],
        ]

        assert get_longest_path(d) == 9


    def test_create_bazar(self):
        """
        Testing the creation of graph with correct number of nodes and edges
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

        peer_id_list = []
        for peer in peers:
            peer_id_list.append(peer.id)
        edges = create_bazar(peer_id_list, False)

        assert len(list(edges)) == 1 # Only 1 edge
        assert len(list(edges)[0]) == 2 # 2 peers only
        assert list(edges)[0] == (0,1)


    def test_create_bazar2(self):
        """
        Testing the creation of graph with correct number of nodes and edges
        """
        ids = {
            "buyer0": "buyer",
            "seller1": "seller",
            "seller2": "seller"
        }

        items = ["fish","salt","boar"]
        items_count = 5
        hostname = "localhost"
        base_path = os.getcwd()
        peers = []
        for id, role in ids.items():
            peer = Peer(id, role, items, items_count, hostname, base_path)
            peers.append(peer)

        peer_id_list = []
        for peer in peers:
            peer_id_list.append(peer.id)
        edges = create_bazar(peer_id_list, False)

        assert len(list(edges)) == 3 # Only 3 edges
        assert len(list(edges)[0]) == 2 # 2 peers only
        assert list(edges)[0] == (0,1)


    def test_get_hop_count(self):
        """
        Testing to see if we are getting the correct hop count
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

        peer_id_list = []
        for peer in peers:
            peer_id_list.append(peer.id)
        edges = create_bazar(peer_id_list, False)
        hop_count = get_hop_count(edges)

        assert hop_count == 1 

    def test_get_hop_count2(self):
        """
        Testing to see if we are getting the correct hop count
        """
        ids = {
            "buyer0": "buyer",
            "seller1": "seller",
            "seller2": "seller"
        }
        items = ["fish","salt","boar"]
        items_count = 5
        hostname = "localhost"
        base_path = os.getcwd()
        peers = []
        for id, role in ids.items():
            peer = Peer(id, role, items, items_count, hostname, base_path)
            peers.append(peer)

        peer_id_list = []
        for peer in peers:
            peer_id_list.append(peer.id)
        edges = create_bazar(peer_id_list, False)
        hop_count = get_hop_count(edges)
        assert hop_count == 1 

    def test_get_hop_count3(self):
        """
        Testing to see if we are getting the correct hop count
        """
        ids = {
            "buyer0": "buyer",
            "seller1": "seller",
            "seller2": "seller",
            "buyer3": "buyer"
        }
        items = ["fish","salt","boar"]
        items_count = 5
        hostname = "localhost"
        base_path = os.getcwd()
        peers = []
        for id, role in ids.items():
            peer = Peer(id, role, items, items_count, hostname, base_path)
            peers.append(peer)

        peer_id_list = []
        for peer in peers:
            peer_id_list.append(peer.id)
        edges = create_bazar(peer_id_list, False)
        hop_count = get_hop_count(edges)

        assert hop_count == 2

    def test_get_hop_count4(self):
        """
        Testing to see if we are getting the correct hop count
        """
        ids = {
            "buyer0": "buyer",
            "seller1": "seller",
            "seller2": "seller",
            "buyer3": "buyer",
            "buyer4": "buyer",
            "buyer5": "buyer",
            "buyer6": "buyer",
            "buyer7": "buyer",
            "buyer8": "buyer",
            "buyer9": "buyer",
            "buyer10": "buyer",
            "buyer11": "buyer",
            "buyer12": "buyer",
            "buyer13": "buyer",

        }
        items = ["fish","salt","boar"]
        items_count = 5
        hostname = "localhost"
        base_path = os.getcwd()
        peers = []
        for id, role in ids.items():
            peer = Peer(id, role, items, items_count, hostname, base_path)
            peers.append(peer)

        peer_id_list = []
        for peer in peers:
            peer_id_list.append(peer.id)
        edges = create_bazar(peer_id_list, False)
        hop_count = get_hop_count(edges)

        assert hop_count == 6
if __name__ == '__main__':
    unittest.main()

