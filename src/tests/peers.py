import Pyro4
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, Lock
import random
import time
import json
import copy
from datetime import datetime

class Peer(Thread):
    def __init__(self,id,role,items, items_count,host,base_path):

        # To inherit the thread class it is necessary to inherit the init and 
        # run method of the thread class
        Thread.__init__(self)
        self.id = id
        self.role  = role
        self.items = items
        self.output = "tmp/output.txt"
        self.items_count = items_count
        self.item = items[random.randint(0, len(items) - 1)]
        self.executor = ThreadPoolExecutor(max_workers=5)
        self.host  = host
        self.hop_count = None
        self.neighbors = {}
        self.lock_item = Lock()
        self.lock_sellers = Lock()
        self.output_array = []
        self.max_items = 10
        self.sellers = []
        self.base_path = base_path
        

    def __str__(self):
        return f'id : {self.id},role : {self.role},nameserver: {self.name_server},items: {self.items}, "hop_count":{self.hop_count})'

    @Pyro4.expose
    def ping(self):
        """
        Test function to check proxy
        """
        return "Pong"
    
    def get_nameserver(self):
        """
        Returns: the endpoint (reference) of the nameserver currently running 
        """
        return Pyro4.locateNS(host=self.host)
    

    def get_all_neighbors(self, peer_id):
        """
        Args:
            peer_id: id of the peer
        Finds the neighbors of the current peer and updates the neighbors dictionary in {id: uri} in pairs 
        """
        time.sleep(2)
        bazar_network = open(f"{self.base_path}/bazaar.json")
        data = json.load(bazar_network)
        try:
            expected_neighbors = data[peer_id]
        except Exception as e:
            print("Loading neighbors for bazaar failed with error {e} Please make sure that the bazar has been created fully")

        for neighbor in expected_neighbors:
            neighbor_uri = self.get_nameserver().lookup(neighbor)
            self.neighbors[neighbor] = neighbor_uri
               
            
    def get_timestamp(self):
        """
        Returns: Current Timestamp
        """
        return datetime.now()

    # Inheriting the run method of thread class
    def run(self):
        """
        Starting point of Peer creation
        Creates objects and registers them as pyro objects. Registers pyro objects to namespace.
        Starts the pyro request loop. Initiates lookups for buyer. Calls the buy method if a seller
        is matched. 
        """
        try:
            # Registering the peer as Pyro5 object
            with Pyro4.Daemon(host = self.host) as daemon:
                try:
                    # Registers peers as pyro object and start daemon thread
                    peer_uri = daemon.register(self)
                    # Registers the pyro object on the nameserver and creates a mapping
                    self.get_nameserver().register(self.id,peer_uri)

                except Exception as e:
                    print(f"Registring to nameserver failed with error {e}")
                if self.role == "buyer":
                    print(f"{self.get_timestamp()} - {self.id} joined the bazar as buyer looking for {self.item}")
                else:
                    print(f"{self.get_timestamp()} - {self.id} joined the bazar as seller selling {self.item}")
                
                # Start the Pyro requestLoop
                self.executor.submit(daemon.requestLoop)

                # Check that all neighbours are healthy before starting buy or sell
                # t1 = Thread(target = self.get_all_neighbors,kwargs={"peer_id":self.id})
              
                self.get_all_neighbors(self.id)
          
                while True and self.role=="buyer":
                    lookups = []
                    
                    for neighbor,uri in self.neighbors.items():
                        neighbor_proxy = Pyro4.Proxy(uri) # getting the uri of the neighbors
                        print(f"{self.get_timestamp()} - Buyer {self.id} issues lookup to {neighbor}")
                        # Creating threads for each neighbor's lookup
                        lookups.append(self.executor.submit(neighbor_proxy.lookup,self.item,self.hop_count,[self.id]))
                    for lookup_request in lookups:
                        # Executing each lookup
                        lookup_request.result()
                    
                    with self.lock_sellers:
                        if self.sellers:
                            # If response from sellers has been received select a random seller
                            id = self.sellers[random.randint(0, len(self.sellers) - 1)]
                            random_seller_id = id["id"]
                            item = id["item"]
                            if item == self.item:
                                print(f"{self.get_timestamp()} - {self.id} picked {random_seller_id} to purchase {self.item}")

                                # get uri of seller
                                seller = Pyro4.Proxy(self.get_nameserver().lookup(random_seller_id))
                                # create a thread to run the buy function of the seller
                                picked_seller = self.executor.submit(seller.buy,self.id)

                                if picked_seller.result(): # run the buy method of the seller
                                    print(self.get_timestamp(), '-', self.id, "bought", self.item, "from", random_seller_id)
                                else:
                                    print(self.get_timestamp(), '-', self.id, "failed to buy", self.item, "from", random_seller_id)
                        self.sellers = []
                        
                        # Choose another random item to buy
                        self.item = self.items[random.randint(0, len(self.items) - 1)]
                        print(f"{self.get_timestamp()} - Buyer {self.id} now picked item {self.item} to buy")
                    
                    # Buyer waiting for a random amount of time before starting a new purchase
                    time.sleep(random.randint(1,3))
                
                while True:
                    time.sleep(1) 
        
        except Exception as e:
            print(f"Something went wrong in run method with exception {e}")
    
    # This method is reponsible for the purchase of an item item logic
    @Pyro4.expose
    def buy(self, buyer_id):
        """
        This function decrements the count of items for the seller. And also allows the
        seller to pick another item when the stock of the current item is finished.
        Args:
            buyer_id: id of the buyer
        Returns:
            boolean: whether the transaction got executed between buyer and seller
        """
        try:
            if self.items_count > 0:
                # Decrement the item count for that seller
                self.items_count -= 1
                print(f"{self.get_timestamp()} - {self.id} sold {self.item} to {buyer_id}")
                print(f"{self.get_timestamp()} - {self.id} now has {self.items_count} {self.item} item(s) remaining")
                return True
            else:
                while True:
                    # pick another random item different than the previous one to sell
                    picked_item = self.items[random.randint(0, len(self.item) - 1)]
                    if self.item!=picked_item:
                        self.item = picked_item
                        break
                # reset the max count of items
                self.items_count = self.max_items
                print(f"{self.get_timestamp()} - {self.id} is now the seller of {self.item}")
                self.output_array.append(f"Seller {self.id} is now the seller of {self.item}.")
                return False
        except Exception as e:
            print(f"Something went wrong in buy with error {e}")


    @Pyro4.expose
    def lookup(self,product_name: str, hop_count: int, search_path):
        """
        This  method is the lookup method which transmits the call to its neighbours
        until a suitable seller for the item is found. Checks if current peer is seller.
        If it is a seller and has the item requested, then reply function of the previous 
        peer is called over a new thread to send the message back to the buyer. If the
        current peer is a buyer or is not selling the item requested, a lookup function
        on all the neighbors of the current peer (except from which the message was received)
        is called to propagate the message forward.
        Args:
            product_name: str
                product demanded by the buyer
            hop_count: int
                number of hops remaining after the previous hop
            search_path: [str]
                list of ids of the peers via which the request arrived
        Return:
            Null
        """
        
        if hop_count < 1:
            # discard message if hopcount = 0
            return
        else:
            # decrement the hop count at every step
            hop_count -= 1
        previous_peer = search_path[-1]
        try:
            if self.role == "seller" and product_name == self.item and self.items_count > 0:
                # If seller found and selling the item call reply
                recipient = Pyro4.Proxy(self.get_nameserver().lookup(previous_peer))
                search_path.pop()
                search_path.insert(0,self.id)
                # if seller is found the reply method is called to send the message back to the buyer
                self.executor.submit(recipient.reply,self.item,search_path)
            else:
                # For each neighbour
                for each_neighbour,uri in self.neighbors.items():
                    # create a deep copy of the search path
                    new_search_path = copy.deepcopy(search_path)
                    # not sending the message back to the peer from which it was received
                    if each_neighbour == previous_peer:  
                        continue
                    neighbor_proxy = Pyro4.Proxy(uri)
                    if self.id not in search_path:
                        search_path.append(self.id)
                    # create new thread and call lookup
                    self.executor.submit(neighbor_proxy.lookup,product_name,hop_count,search_path)
 
        except Exception as e:
            print(f"Something went wrong in lookup with exception {e}. Peers still loading.")


    @Pyro4.expose
    def reply(self, item, id_list):
        """
        This function handles the reply being sent by the matched seller back to the buyer.
        If a seller is found, then the id is appended to a list of sellers for the buyer.
        The id_list contains the return path of the message from the seller to the buyer.
        if there is only 1 element left, it is the seller's id. If there are more than 1
        elements in the id_list, message still hasn't reached the buyer and the reply method
        of the previous peer will be called.
        Args:
            id_list: the list of ids of the peer through which the reply should go through
        Returns:
            Null
        """
        try:
            if id_list and len(id_list) == 1:

                print(self.get_timestamp(), '-', self.id, "got a match reply from", id_list[0])
                # adding the seller to the list of matched sellers
                with self.lock_sellers:
                    self.sellers.append({"id":id_list[0],"item":item})

            elif id_list and len(id_list) > 1:
                # Calling the reply method of the previous peer
                recipient_id = id_list.pop()
                with Pyro4.Proxy(self.neighbors[recipient_id]) as recipient:
                    self.executor.submit(recipient.reply, item, id_list)

        except(Exception) as e:
            print(f"Something went wrong while trying to fecth the reply with error {e}")

        



        