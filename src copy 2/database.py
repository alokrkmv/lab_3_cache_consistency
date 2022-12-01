from pymongo import MongoClient
class DbHandler:
    
    def __init__(self,  host="localhost", port_number = 27017):
        self.client = MongoClient()
        try:
            self.test_database = self.client["test_db"]
            self.test_collection = self.test_database["test_collection"]
            self.mydatabase = self.client["bazaar_info"]
            self.collection = self.mydatabase["trader_info"]
            self.transactions = self.mydatabase["transactions"]
            test_message = {"test_message":"testing"}
            newvalues = { "$set": { "test_message": "testing_new" } }
            self.test_collection.update_one(test_message,newvalues,upsert = True)

        except Exception as e:
            print(f"Something went Wrong while connecting to database with error {e}")
        
    
    def reset_database(self):
        """
        Removes all the previous run data from collections
        """
        self.collection.drop()
        self.transactions.drop()

    # This function is called by init to check if database connection is succesful
    def test_connection(self):
        """
        Checks if the connection to the database is successful
        """
        try:
            res = self.test_collection.find_one({"test_message": "testing_new"})
            if "test_message" in res:
                return True
            else:
                False
        except Exception as e:
            print(f"Something went wrong while connecting to MongoDB server with exception {e}")
            return False


    def insert_into_database(self, data):
        """
        Insert a single entity into database
        If already present then update
        Determined by upsert flag
        """
        try:
            query = {"seller_id":data["seller_id"]}
            newvalues = { "$set": data }
            self.collection.update_one(query,newvalues,upsert = True)
        except Exception as e:
            print(f"Something went Wrong while inserting into database {self.mydatabase} with error {e}")

    # This functions fetches all data from the trader_info_collection
    def fetch_all_from_database(self):
        """
        This functions fetches all data from the trader_info_collection
        """
        try:
            sellers_data = []
            data = self.collection.find()
            for d in data:
                sellers_data.append(d)
            return sellers_data
        except Exception as e:
            print(f"Something went wrong while trying to fetch all data with exception {e}")




    # This function fetches data of a particular seller from collection
    def fetch_one_from_database(self,seller_id):
        """
        This function fetches data of a particular seller from collection
        """
        try:
            seller_data = self.collection.find_one({"seller_id":seller_id})
            return seller_data
        except Exception as e:
            print(f"Something went wrong while trying to fetch data for {seller_id} with exception {e}")
   
    # Fetches all info of a seller selling particular item.
    def find_seller_by_item(self, item, trader_id, seller_clock):
        """
        This function fetches the seller that has a particular item and has the lowest clock value
        """
        all_data = self.fetch_all_from_database()
        min_clock = float("inf")
        min_seller = None
         
        
        sellers = []

        seller_dict =   {}    
            
        for data in all_data:
            if data["item"]==item and data["seller_id"]!=trader_id:
                if len(seller_clock)<=0:
                    return data
                sellers.append(data["seller_id"])
                seller_dict[data["seller_id"]] = data

        for seller_id, clock in seller_clock:
            if seller_id not in sellers:
                continue
            if clock <= min_clock:
                min_clock = clock
                min_seller = seller_id

        if min_seller == None and len(sellers)>0:
            return seller_dict[sellers[0]]
        if len(sellers)<=0:
            return None
        return seller_dict[min_seller]

    # Saves pending transactions of the bazaar
    def save_transactions(self, item):
        """
        Saves pending transactions of the bazaar
        """
        try:
            query = {"seller_id":"trader"}
            newvalues = { "$set": item }
            self.transactions.update_one(query,newvalues,upsert = True)
        except Exception as e:
            print(f"Something went Wrong while inserting into database {self.mydatabase} with error {e}")
    
    def fetch_pending_transactions(self):
        """
        Fetches the pending transactions
        """
        try:
            transactions = self.transactions.find_one({"seller_id":"trader"})
            return transactions
        except Exception as e:
            print(f"Something went Wrong while inserting into database {self.mydatabase} with error {e}")


    def delete_one(self, query):
        """
        Deletes ther data for a single entry from the database for the given query
        """
        try:
            self.transactions.delete_one(query)
        except Exception as e:
            print(f"Something went Wrong while deleting item from {self.mydatabase} with error {e}")




                

    