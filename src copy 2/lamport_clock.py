class LamportClock:
    #: The clocks current value.
    value = 0

    def __init__(self, initial_value=0):
        self.value = initial_value

    def adjust(self, other):
        """
        Args:
            other: int (value of the clock of another peer)
        Sets the clock value of the current peer to the max val between itself and the clock value of another peer
        """
        self.value = max(self.value, other)

    def forward(self):
        """
        Increments the clock value of a peer
        """
        self.value += 1
        return self.value