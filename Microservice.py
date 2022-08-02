import math
import random
import string
from MicroserviceContainer import *


class Microservice:
    """
        Models a microservice comprised of several redundant containers with uniform failure functions
    """

    def __init__(self, cost, num_containers=0, t0=0, name=None):
        """
            Creates a new model of a microservice
            num_containers - number of containers to spawn with
            t0 - current global time
            name - optional name for the microservice, default will randomise
        """
        self.cost = cost
        self.containers = []

        if name is None:
            self.name = "MS_" + ''.join(random.choice(string.digits) for i in range(5))
        else:
            self.name = name

        for i in range(num_containers):
            self.spawn_container(t0=t0)

    def failure_function(self, t):
        """
            The failure function of the microservice container (F)
            This function is to be overridden in the child implementation

            t - local time
        """

        raise NotImplementedError("Failure function not implemented")

    def random_time_to_failure_given_survival_time(self, t):
        """
            Returns a random value from the distribution given the container has survived until t
            t - local time
        """

        raise NotImplementedError("Random time to failure function not implemented")

    def spawn_container(self, t0=0, name=None):
        """
            Spawns a new redundant container in the microservice
            t0 - the start time in global time
            name - optional name for the container, default will randomise
        """
        container = MicroserviceContainer(self.failure_function, self.random_time_to_failure_given_survival_time, t0, name)
        self.containers.append(container)
        return container

    def probability_of_failure(self, t, delta):
        """
            Returns the value of the failure function for the microservice
            t - global time
            delta - offset
        """
        failure_function_values = [x.probability_of_failure(t, delta) for x in self.containers]

        # The probability that the microservice fails is the probability of all redundant containers failing
        return math.prod(failure_function_values)

    def __str__(self):
        s = self.__class__.__name__ + "::" + self.name + f" (Cost={self.cost}): "
        if len(self.containers) > 0:
             s += ", ".join(str(x) for x in self.containers)
        else:
            s += "No containers"
        return s