class SpotMarketProvider:
    def __init__(self):
        pass

    def _cost_of_failure_per_second(self, t):
        return 1

    def cost_of_failure(self, t, delta):
        """
            Returns the cost of a system failure if it occurred at time t (global time)
            Abstract
        """
        return self._cost_of_failure_per_second(t) * delta

    def _spot_price_per_second(self, t):
        """
            Returns the spot market price per second at time t
            Abstract
        """
        return 1

    def spot_price(self, t, delta):
        """
        Returns the spot price of running for a single time quantum
        """
        return self._spot_price_per_second(t) * delta
