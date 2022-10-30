import math
import string
import random


class MicroserviceContainer:
    """
        Models a specific instance of a microservice container
    """
    STATE_ACTIVE = 0
    STATE_FAILED = 1

    def __init__(self, failure_function, local_failure_time, t0=0, name=None):
        """
            Creates a new microservice container
            failure_function - a function of the form (t, delta) => [0,1] (Pr(failure))
            failure_time - the local failure time for the microservice container
            t0 - starting time of the container
            name - optional name for the container, default will randomise
        """
        self._failure_function = failure_function
        self.local_failure_time = local_failure_time
        self._t0 = t0
        if name is None:
            name = "CONTAINER_" + ''.join(random.choice(string.digits) for i in range(5))
        self.name = name

        self.state = MicroserviceContainer.STATE_ACTIVE

    def probability_of_failure(self, t, delta):
        """
            Returns the probability that the container will fail in the given interval [t, t+delta]
            A failed container will always return 1
            t - global time
            delta - size of the interval
        """
        if self.state == MicroserviceContainer.STATE_ACTIVE:
            # Compute the probability of failure in the given interval, given it has survived until now (by Bayes)
            prob = (self._failure_function(self.global_to_local_time(t + delta)) -
                    self._failure_function(self.global_to_local_time(t))) / (1 - self._failure_function(self.global_to_local_time(t)))
            return prob
        else:
            return 1 # If the container has failed, it will remain failed

    def global_to_local_time(self, t):
        """
            Converts the global clock to take into account the starting time of the microservice container
        """
        return t - self._t0

    def __str__(self):
        return self.name + " (" + str(self.state) + ")"
