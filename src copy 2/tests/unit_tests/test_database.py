import sys
sys.path.append("..")
import unittest
from database import DbHandler

class TestDatabase(unittest.TestCase):

    def test_db_connection(sellf):
        """
        Database connection successful
        """
        db = DbHandler()
        assert(db.test_connection != None)

    
    def test_find_seller_by_item(self):
        """
        This test checks the tie breaking criteria for selection of the seller for a given item with minimum clock value.
        """
        
        def find_seller_by_item(item, trader_id, seller_clock):
            """
            item: string
            trader_id: string
            """
            all_data = [
                {
                    'item': 'fish',
                    'seller_id': 'seller0',
                    'count': 10
                },
                {
                    'item': 'fish',
                    'seller_id': 'seller4',
                    'count': 10
                },
                {
                    'item': 'boar',
                    'seller_id': 'seller8',
                    'count': 10
                }
            ]
    
        
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
        


        assert (find_seller_by_item('fish', 'seller8', [("seller0", 3), ("seller4", 4), ("seller8", 5)]) == {
                    'item': 'fish',
                    'seller_id': 'seller0',
                    'count': 10
                })

if __name__ == '__main__':
    unittest.main()