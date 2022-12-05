import Pyro4
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, Lock
from multiprocessing import Process
import random
import time
import json
import copy
from database import DbHandler
from lamport_clock import LamportClock

from datetime import datetime


class Peer(Process):
    def __init__(self,id,role,items, items_count,host,base_path, number_of_traders):

        # To inherit the thread class it is necessary to inherit the init and 
        # run method of the thread class
        Process.__init__(self)
        self.id = id
        self.role  = role
        self.items = items
        self.output = "tmp/output.txt"
        self.items_count = items_count
        self.item = items[random.randint(0, len(items) - 1)]
        self.executor = ThreadPoolExecutor(max_workers=20)
        self.host  = host
        self.lock_item = Lock()
        self.lock_sellers = Lock()
        self.output_array = []
        self.max_items = 10
        self.sellers = []
        self.base_path = base_path
        self.neighbors = []
        self.received_ok_message = False
        self.received_won_message = False
        self.send_winning_message = False
        self.current_trader_id = [] # None
        self.election_flag = False
        self.election_lock = Lock()
        self.winning_lock = Lock()
        self.price = None
        self.has_deposited = False
        self.db = DbHandler()
        self.has_deposited_lock = Lock()
        self.trading_queue = []
        self.item_lock = []
        self.trading_lock = Lock()
        self.heartbeat_counter = 0
        self.heatbeat_lock = Lock()
        self.smallest_buyer = "buyer0"
        self.number_of_traders = number_of_traders

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
    # Get pyro4 uri for each neighbor
    def get_uri_from_id(self,peer_id):
        neighbor_uri = self.get_nameserver().lookup(peer_id)
        neighbor_proxy = Pyro4.Proxy(neighbor_uri)
        return neighbor_proxy

    def check_higher_id(self,id_2):
        id_1_index = int(self.id[-1])
        id_2_index = int(id_2[-1])

        return True if id_2_index>id_1_index else False

    
                    
            
    def forward_win_message(self):
        """
        This function prints the winning message for the peer that has won the election. It sets the received_won_message flag to True, to ensure
        there is no re-election. Sets the election_flag to False, to indicate that election is over. sets current_trader_id to the peer's id who won
        the election. And sets the role to trader. It fetches the pending requests from the buyers before the previous trader died. Then it 
        sends a win message to all the peers in the network. Finally a high number of threads are created for the trader and the begin trading function
        is called.
        """

        print(f"{self.get_timestamp()} : Dear buyers and sellers, My ID is {self.id}, and I am the new coordinator")
  
        self.received_won_message = True
        self.election_flag = False
        self.current_trader_id.append(self.id)
        self.role = "trader"

        pending_transactions = self.db.fetch_pending_transactions()
        if pending_transactions !=None:
            self.trading_queue = pending_transactions["data"][:]
        for neighbor in self.neighbors:
            self.executor.submit(self.get_uri_from_id(neighbor).send_election_message ,"won",self.id)

        # Increase the number of workers for trader as Trader has a lot to do in 
        # current architecture
        self.executor = ThreadPoolExecutor(max_workers=50)

        print(f"{self.get_timestamp()} : Trader{self.current_trader_id.index(self.id)} is ready for product registration")
        self.executor.submit(self.begin_trading)
        



    def seller_loop(self,):
        # send_sale_message logic`
        try:
            while True:
                time.sleep(50)
                print(f"{self.get_timestamp()} : Executing seller loop")
                while True:
                    picked_item = self.items[random.randint(0, len(self.items) - 1)]
                    if self.item!=picked_item:
                        self.item = picked_item
                        break
                print(f"{self.get_timestamp()} : {self.id} has picked item {self.item} to sell")
                self.has_deposited_lock.acquire()
                self.has_deposited = False
                self.has_deposited_lock.release()
                self.begin_trading()
        except Exception as e:
            print(f"Something went wrong with seller_loop with error {e}")

    # This method elects the leader using Bully algorithm
    @Pyro4.expose
    def elect_leader(self):
        """
        Reverse the role of the current trader (if any)
        The peer 2 starts the election. It gets the uri of all the neighbor peers (all peers in this case) and sets the election_flag to True.
        For all the peers with higher id, a message to "elect leader" is sent. And then it waits fo 5 seconds before checking if the received_ok_message
        flag is set or not. If it is False, and there is no win message from any peer, then the current peer wins the election.
        If there are no higher peers then the current peer wins and forwards the win message.
        For the highest peer, if that peer doesn't receives any response of "OK" after sending the elect leader message, then that highest peer
        wins and forwards the win message.
        """
        try:
            print(f"{self.get_timestamp()} : {self.id} has started the election")
            higher_peers = []
            
            for neigh_id in self.neighbors:
                if neigh_id in self.current_trader_id:
                    continue
                if self.check_higher_id(neigh_id):
                    higher_peers.append(self.get_uri_from_id(neigh_id))
            if len(higher_peers)>0:
                # Acquire the election lock and set election running status to true
                self.election_lock.acquire()
                self.election_flag = True 
                self.election_lock.release()
            

                for higher_peer in higher_peers:
                    self.executor.submit(higher_peer.send_election_message ,"elect_leader",self.id)
                time.sleep(5)
                if self.received_ok_message == False and self.received_won_message == False:
                    print(f"entering this if statement for {self.id}")
                    self.winning_lock.acquire()
                    self.send_winning_message = True
                    self.forward_win_message()
                    self.winning_lock.release()
    
            else:
                self.winning_lock.acquire()
                self.send_winning_message = True
                self.forward_win_message()
                self.winning_lock.release()
        except Exception as e:
            print(f"{self.get_timestamp()} : Something went wrong while {self.id} tried to start election with error {e}")
        


    @Pyro4.expose
    def send_heartbeat(self):
        if self.heartbeat_counter>5 and self.current_trader_id.index(self.id)==1:
            return "dead"
        else:
            return "alive"
    @Pyro4.expose
    def send_election_message(self,message,sender):
        """
        Args:
            message: string (the message sent by a peer)
            sender: string peeer id
        This function handles three types of messages.
        
        elect_leader : If this message is received by the peer, then it will first respond to the sender by an ok message 
        and forwards the election message to its neighbors with a higher id. If there are higher peers, then the election_flag is set to True
        After forwarding the election message, the peer waits for 5 seconds to check if received_ok_message and received_won_message flags are
        set to True or not. If they are set to true then the current peer drops from the election. Else it sends a win message.
        
        OK: If a peer receives the OK message it drops from the election since there are higher peers who are alive
        
        won: If won message is recieved from any peer, then the received_won_message flag is set to trye, current trader id is set to the 
        id of the sender, election_flag is set to False to mark the ending of election. Then begin_trading function is called.
        """
        try:
            if message == "elect_leader" and self.id not in self.current_trader_id:
                self.executor.submit(self.get_uri_from_id(sender).send_election_message,"OK",sender)
                # If the peer haven't taken part in election till now only then it will take part in an election
                if not self.received_ok_message and not self.received_won_message:
                    higher_peers = []
                    for neigh_id in self.neighbors:
                        if neigh_id in self.current_trader_id:
                            continue
                        if self.check_higher_id(neigh_id):
                            higher_peers.append(self.get_uri_from_id(neigh_id))
                    if len(higher_peers)>0:
                        # Acquire the election lock and set election running status to true
                        self.election_lock.acquire()
                        self.election_flag = True
                        self.election_lock.release()
                        
                        for higher_peer in higher_peers:
                            self.executor.submit(higher_peer.send_election_message ,"elect_leader",self.id)
                        
                    else:
                        if self.received_ok_message == False and self.received_won_message == False:
                            self.winning_lock.acquire()
                            self.send_winning_message = True
                            self.forward_win_message()
                            self.winning_lock.release()
            elif message == "OK":
                self.received_ok_message = True
            elif message == "won":
                print(f"{self.get_timestamp()} : {self.id} received message won from {sender} and recognizes {sender} as the new coordinator of the bazaar")
                self.winning_lock.acquire()
                self.received_won_message = True
                if sender not in self.current_trader_id:
                    self.current_trader_id.append(sender)
                self.election_flag = False # 
                self.winning_lock.release()
                print(f"{self.get_timestamp()} : {self.id} is ready to trade")
                self.executor.submit(self.begin_trading)
                #### Need to define the seller loop
                if self.role == "seller":
                    self.executor.submit(self.seller_loop())
        except Exception as e:
            print(f"{self.get_timestamp()} : Something went wrong with error {e}")

    @Pyro4.expose
    def begin_trading(self):
        """
        This method resets all the flags to False to create a fresh bazar.
        It first checks if the seller has deposited its items to the trader, if not then deposit the items to the trader and the has_deposited
        flag to True. 
        If the role is trader, then start the trader loop
        If the rolw is buyer then start the buyer loop
        """
        time.sleep(10)
        self.winning_lock.acquire()
        self.received_won_message = False
        self.received_ok_message = False
        self.send_winning_message = False
        self.heartbeat_counter = 0
        self.election_flag = False
        self.winning_lock.release()
        if len(self.current_trader_id) < self.number_of_traders:
            if self.id == self.smallest_buyer:
                print(f"Start re-election!! Traders elected till now {self.current_trader_id}")
                self.elect_leader()
            return

        try:
            # Set all election related flags to false
            print(f"{self.get_timestamp()} : Waiting 15s before starting trading")
            time.sleep(15)

            # When trading begins seller will deposit all their items to trader if they haven't done so
            
            if self.role == "seller" and not self.has_deposited:
                trader = self.current_trader_id[random.randint(0, len(self.current_trader_id)-1)]
                trader_proxy = self.get_uri_from_id(trader)
                item = self.item
                count  = self.items_count
                price = self.price

                seller_data = {"seller_id":self.id, "count":count, "price":price, "item":item}
                registration_result = self.executor.submit(trader_proxy.register_product, seller_data, self.id)
                res = registration_result.result()
                if res:
                    print(f"{self.get_timestamp()} : {self.id} registered their product {self.item} with trader{self.current_trader_id.index(trader)}")
                    self.has_deposited_lock.acquire()
                    self.has_deposited = True
                    self.has_deposited_lock.release()
                else:
                    print(f"Something went wrong while registering the product with trader{self.current_trader_id.index(trader)}. Retrying!!!")
                    self.begin_trading()
            # Trader loop will take care of purchase request from buyer
            elif self.role == "trader":
                time.sleep(10)
                print(f"{self.get_timestamp()} : Trader{self.current_trader_id.index(self.id)} aka {self.id} - is ready to trade")
                print(f"{self.get_timestamp()} : {len(self.current_trader_id)} trader(s) in the bazar, trading can begin!!!")
                self.executor.submit(self.trader_loop)
            # If role is of buyer then start buyer loop and keep on buying product
            elif self.role == "buyer":
                # Wait for sellers to register their product with trader
                time.sleep(10)
                self.executor.submit(self.buyer_loop)
        except Exception as e:
            print(f"{self.get_timestamp()} : Something went wrong while starting to trade for {self.id} with error {e}")
    
    @Pyro4.expose              
    def add_to_trading_queue(self, buyer_id, item):
        self.trading_queue.append((item, buyer_id))
     


    # This method starts buyer loop for buyer using which they can buy any product
    def buyer_loop(self):
        """
        This function keeps checking for the heartbeat of the trader. If the response is dead, then the peer sets the election_flag to True.
        The peer with the smallest id restarts the election.
        In every buy loop, the buyer broadcasts its clock to all the peers, adds the item that it wants to buy to the trading queue and then waits
        for 10 seconds to send another request.
        """
        while True:
            try:
                trader = self.current_trader_id[random.randint(0, len(self.current_trader_id)-1)]
                trader = self.get_uri_from_id(trader)
                # self.broadcast_lamport_clock()
                # self.clock.forward()
                # clock_value = self.clock.value
                self.executor.submit(trader.add_to_trading_queue, self.id, self.item)
                time.sleep(10)
                
                self.item = self.items[random.randint(0, len(self.items) - 1)]
            except Exception as e:
                print(f"Something went wrong in the buyer loop with error {e} ")
            
        
    @Pyro4.expose
    def send_purchase_message(self, seller_id, item):
        """
        Args:
            seller_id: id of the seller who sold that item
            item: itme which was sold by the seller
        This function prints the message about the item that the buyer has bought
        The buyer then selects another item to buy
        """
        print(f"{self.get_timestamp()} : {self.id} purchased {item} from {seller_id}")
        self.item_lock.aquire()
        self.item = self.items[random.randint(0, len(self.items) - 1)]
        self.item_lock.release()
        print(f"{self.get_timestamp()} : {self.id} has now picked item {self.item} to purchase")

    @Pyro4.expose
    def get_dead_trader_queue(self):
        return self.trading_queue

    @Pyro4.expose
    def send_death_message(self):
        print(f"{self.id} received death message of {self.current_trader_id[1]} from {self.current_trader_id[0]}!!! resetting trader_queue")
        only_trader = self.current_trader_id[0]
        self.current_trader_id = []
        self.current_trader_id.append(only_trader)

    
        
        

    # Trader will sell the product to buyer using this function they will also send a message to the seller 
    # whose product was sold with remaining number of products and commission amount
    def trader_loop(self):
        """
        This function runs the trader's loop. It first writes all the pending transactions to the disk to keep a track of the pending transactions
        to ensure that in an untimely death of the trader, the next trader will be able to pick up the remaining transactions.
        Then it iterates of the trading queue to get the buyer id which has the lowest clock value. Then it processes that request. It first adjusts
        the buyer's clock and then finds the seller that has this item and has the lowest clock. After it has successfully found the seller, it prints
        a message for the successful trade of item between buyer and seller. The sellers items are then decreased by 1 and the value is then updated
        in the database. It sends a message which tells which seller's item was sold and how much did the seller earn. Also prints the id of the buyer
        that bought the item.
        """
        while True:
            if self.election_flag:
                break
            try:
                if len(self.trading_queue)>0:
                    # Write all the pending transactions to the disk
                    second_trader = None
                    if len(self.current_trader_id)>1 and self.id!=self.current_trader_id[1]:
                        second_trader = self.current_trader_id[1]
                        second_trader_proxy = self.get_uri_from_id(second_trader)
                        res = self.executor.submit(second_trader_proxy.send_heartbeat)
                        result = res.result()
                        if result == "dead":
                            
                            print(f"Trader1 aka {second_trader} is dead!!! Now Trader0 aka {self.current_trader_id[0]} is only trader of the bazaar!!!")
                            self.trading_queue.extend(second_trader_proxy.get_dead_trader_queue())
                            self.current_trader_id = []
                            self.current_trader_id.append(self.id)
                            neighbors = self.neighbors
                            for neighbor in neighbors:
                                if neighbor == second_trader:
                                    continue
                                else:
                                    neighbor_proxy = self.get_uri_from_id(neighbor)
                                    self.executor.submit(neighbor_proxy.send_death_message)
                            # Let other peers reset their trading queue
                            time.sleep(15)

                    pending_transactions = {"data":self.trading_queue}
                    try:
                        thread = Thread(target=self.db.save_transactions, args=(pending_transactions,))
                        thread.start()
                
                    except Exception as e:
                        print("Something went wrong while trying to write pending transactions to disk with error {e}")
                    item, buyer_id = self.trading_queue.pop(0)
                    
                    print(f"{self.get_timestamp()} : Trader{self.current_trader_id.index(self.id)} got a purchase request for item {item} from {buyer_id}")
                    data = self.db.find_seller_by_item(item, self.id, self.sellers)
                   
                    
                    if data == None:
                        print(f"{self.get_timestamp()} : {item} is not available for sell in the bazaar right now")
                        continue
                    seller_id = data["seller_id"]
                    price = data["price"]
                    count = data["count"]
                    

                    seller_proxy = self.get_uri_from_id(seller_id)
                    buyer_proxy = self.get_uri_from_id(buyer_id)

                    self.db.insert_into_database({"seller_id":seller_id, "count":count-1, "price":price, "item":item})
                    # Upon each succesful trade increase the hearbeat counter by 1
                    self.heatbeat_lock.acquire()
                    self.heartbeat_counter+=1
                    self.heatbeat_lock.release()
                    print(f"{self.get_timestamp()} : Current Trader{self.current_trader_id.index(self.id)} of the bazar is {self.id}")
                    self.executor.submit(seller_proxy.send_sale_message, item, round(0.8*price,2), count-1, buyer_id, False)
                    self.executor.submit(buyer_proxy.send_purchase_message, seller_id, item)
  
              
            except Exception as e:
                print(f"{self.get_timestamp()} : Registering product for {seller_id} failed with error{e}")
                return False

    @Pyro4.expose
    def send_sale_message(self, item, commission, count, buyer_id, zero_flag):
        """
        This function prints the item that a seller sold and the money that it earned. Also prints the remaining number of items.
        The clock of the seller is forwarded by 1, and the new value of the clock is updated with the trader.
        If the count of items is 0, then a message saying the seller is out of stock is printed. Then the seller picks up another item.
        After the item is picked, has_deposited flag is set to false, signifying that the seller is yet to deposit its items with the trader.
        """
        if count>=0:
            print(f"{self.get_timestamp()} : {self.id} has sold {item} to {buyer_id} and earned {commission} $")
            print(f"{self.get_timestamp()} : {self.id} has {count} {item} left")

        if count <=0:
            print(f"{self.get_timestamp()} : {self.id} is out of stock for item {item}")
            while True:
                picked_item = self.items[random.randint(0, len(self.item) - 1)]
                if self.item!=picked_item:
                    self.item = picked_item
                    break
            print(f"{self.get_timestamp()} : {self.id} has picked item {self.item} to sell")
            self.has_deposited_lock.acquire()
            self.has_deposited = False
            self.has_deposited_lock.release()
            self.begin_trading()


    # This method registers products of each seller
    @Pyro4.expose
    def register_product(self, seller_info, seller_id):
        """
        This method registers the product of the seller, and adds it to the database
        """
        try:
            self.db.insert_into_database(seller_info)
            seller_data = self.db.fetch_one_from_database(seller_id)
            if seller_data == None or seller_data["count"] == 0:
                return False
            return True
        except Exception as e:
            print(f"Registering product for {seller_id} failed with error{e}")
            return False

                       
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
                    print(f"{self.get_timestamp()} : {self.id} joined the bazar as buyer looking for {self.item}")
                else:
                    print(f"{self.get_timestamp()} : {self.id} joined the bazar as seller selling {self.item}")
                
                # Start the Pyro requestLoop
                self.executor.submit(daemon.requestLoop)
                # Sleep for sometime so that all peers join Bazaar
                time.sleep(4)
                if int(self.id[-1])==2:
                    self.elect_leader()
                while True:
                    time.sleep(1)
        
        except Exception as e:
            print(f"Something went wrong in run method with exception {e}")
    
    