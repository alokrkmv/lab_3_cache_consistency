from start_nameserver import start_nameserver
import unittest
import Pyro4

class TestNameserver(unittest.TestCase):
    """
    Checking is nameserver is up and running
    """
    def test_check_nameserver_status(self):
        start_nameserver()
        ns = Pyro4.locateNS(host="localhost")

        assert ns != None


if __name__ == '__main__':
    unittest.main()