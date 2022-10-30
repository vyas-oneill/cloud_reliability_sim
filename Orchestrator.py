from MicroserviceContainer import *
import copy


class Orchestrator:
    def __init__(self, cost_of_failure=0, trace=False):
        self._trace = trace
        self._cost_of_failure = cost_of_failure

    def _remove_failed_containers(self, cloud):
        """
            Removes failed containers from the cloud
        """
        for microservice in cloud.microservices:
            new_containers = []
            for i in range(len(microservice.containers)):
                container = microservice.containers[i]
                if container.state == MicroserviceContainer.STATE_ACTIVE:
                    new_containers.append(container)
                else:
                    self.print_trace(f"Removed {container} from the cloud due to failure.")

            microservice.containers = new_containers

    def _ensure_at_least_one_container(self, cloud, t):
        """
            Ensures that the cloud contains at least one container in each microservice
        """
        for microservice in cloud.microservices:
            if len(microservice.containers) == 0:
                microservice.spawn_container(t0=t)
                self.print_trace(f"Microservice {microservice} was in a failure state. Spawned one container.")

    def select_microservice_for_redundancy(self, cloud, t, delta):
        """
            Selects which microservice container provides the highest utility function
            Returns the index into cloud.microservices
            If no microservice provides a positive utility, None is returned
        """
        current_expected_cost_of_failure = self.expected_cost_of_failure(t, delta, cloud)
        highest_utility = 0
        selected_microservice = None
        for i in range(len(cloud.microservices)):
            proposed_cloud = copy.deepcopy(cloud)
            proposed_cloud.microservices[i].spawn_container(t0=t)
            proposed_expected_cost_of_failure = self.expected_cost_of_failure(t, delta, proposed_cloud)

            # Compute the utility function
            utility = current_expected_cost_of_failure - proposed_expected_cost_of_failure - cloud.microservices[i].cost * delta
            if utility > highest_utility:
                highest_utility = utility
                selected_microservice = i

        return selected_microservice

    def expected_cost_of_failure(self, t, delta, cloud):
        """
            Returns the expected cost of failure
            t - global time
            delta - the period of time for which to compute expected cost
            cloud - the cloud on which to compute
        """
        return self._cost_of_failure * delta * cloud.probability_of_failure(t, delta)

    def orchestrate(self, cloud, t):
        """
            Runs the orchestration algorithm given the current time
            Returns the cost incurred spawning new instances
        """
        self._remove_failed_containers(cloud)
        return 0

    def print_trace(self, msg):
        if self._trace:
            print("[ORCH] " + msg)
