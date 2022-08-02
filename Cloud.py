import math


class Cloud:
    """
        Represents a cloud providing some service (task T) comprised of redundant microservices
    """

    def __init__(self, microservices):
        self.microservices = microservices.copy()

    def probability_of_failure(self, t, delta):
        """
            Computes the task failure function
            t - global time
            delta - offset
        """
        microservice_reliabilities = [1 - x.probability_of_failure(t, delta) for x in self.microservices]

        # The probability of task not failing is the probability that all microservices are in acceptable states
        task_reliability = math.prod(microservice_reliabilities)

        # The probability of failure of the task is 1 - P(task_reliability)
        return 1 - task_reliability

    def __str__(self):
        return " | ".join(str(x) for x in self.microservices)
