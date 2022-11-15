from pymongo import MongoClient
class DbHandler:
    
    def __init__(self,  host="localhost", port_number = 27017):
        self.client = MongoClient()
        try:
            self.test_database = self.client["test_db"]
            self.test_collection = self.test_database["test_collection"]
            self.mydatabase = self.client["bazaar_info"]
            self.collection = self.mydatabase["trader_info"]
            test_message = {"test_message":"testing"}
            newvalues = { "$set": { "test_message": "testing_new" } }
            self.test_collection.update_one(test_message,newvalues,upsert = True)

        except Exception as e:
            print(f"Something went Wrong while connecting to database with error {e}")
        
    
    def reset_database(self):
        
        self.collection.drop()

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
    # # Updates the data for an individual seller
    # def update_one(self,seller_id, new_data):

    #     try:
    #         filter = { 'seller_id': seller_id }
            
    #         # Values to be updated.
    #         newvalues = { "$set": new_data }
    #         self.collection.update_one(filter,newvalues)
    #     except Exception as e:
    #         print(f"Something went wrong while trying to update the data for {seller_id} with exception {e}")

    # Fetches all info of a seller selling particular item.

    def find_seller_by_item(self, item):
        
        all_data = self.fetch_all_from_database()
  
    
        for data in all_data:
        
        
            if data["item"]==item:
               
                return data
        return None




                

    