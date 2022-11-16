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
        
        self.collection.drop()
        self.transactions.drop()

    # This function is called by init to check if database connection is succesful
    def test_connection(self):
        try:
            res = self.test_collection.find_one({"test_message": "testing_new"})
            if "test_message" in res:
                return True
            else:
                False
        except Exception as e:
            print(f"Something went wrong while connecting to MongoDB server with exception {e}")
            return False

    # Insert a single entity into database
    # If already present then update
    # Determined by upsert flag
    def insert_into_database(self, data):
        try:
            query = {"seller_id":data["seller_id"]}
            newvalues = { "$set": data }
            self.collection.update_one(query,newvalues,upsert = True)
        except Exception as e:
            print(f"Something went Wrong while inserting into database {self.mydatabase} with error {e}")

    # This functions fetches all data from the trader_info_collection
    def fetch_all_from_database(self):
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
        try:
            seller_data = self.collection.find_one({"seller_id":seller_id})
            return seller_data
        except Exception as e:
            print(f"Something went wrong while trying to fetch data for {seller_id} with exception {e}")
   
    # Fetches all info of a seller selling particular item.

    def find_seller_by_item(self, item, trader_id):
        
        all_data = self.fetch_all_from_database()
  
    
        for data in all_data:
        
        
            if data["item"]==item and data["seller_id"]!=trader_id:
               
                return data
        return None

    # Saves pending transactions of the bazaar

    def save_transactions(self, item):
        try:
            query = {"seller_id":"trader"}
            newvalues = { "$set": item }
            self.transactions.update_one(query,newvalues,upsert = True)
        except Exception as e:
            print(f"Something went Wrong while inserting into database {self.mydatabase} with error {e}")
    
    def fetch_pending_transactions(self):
        try:
            transactions = self.transactions.find_one({"seller_id":"trader"})
            return transactions
        except Exception as e:
            print(f"Something went Wrong while inserting into database {self.mydatabase} with error {e}")


    def delete_one(self, query):
        try:
            self.transactions.delete_one(query)
        except Exception as e:
            print(f"Something went Wrong while deleting item from {self.mydatabase} with error {e}")




                

    