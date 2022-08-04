import math

from MicroserviceContainer import *
import random
import numpy


class Simulator:
    def __init__(self, orchestrator, cloud, sim_clock_step=0.01, orchestrator_run_period=0.01, trace=False):
        """
            Creates a new simulation
            orchestrator: the Orchestrator object
            cloud: a Cloud object
            sim_clock_step: by how much the simulation clock increments each iteration
            orchestrator_run_period: how often to run the orchestrator
            trace: whether to print debug messages
        """

        # Simulation parameters
        self.orchestrator = orchestrator
        self.cloud = cloud
        self._sim_clock_step = sim_clock_step
        self._t = 0
        self._time_since_orchestrator = math.inf
        self._orchestrator_run_period = orchestrator_run_period
        self.orchestrator._trace = self._trace = trace

        # Outputs
        self._task_failure_probability = [] # Probability of task failing in the given interval [index * sim_clock_step, index * sim_clock_step + sim_clock_step]
        self._expected_costs_of_failure = [] # Expected cost of failure in the given interval [index * sim_clock_step, index * sim_clock_step + sim_clock_step]
        self._failed_containers = [] # Tuples of (local time of failure, MicroserviceContainer)
        self._actual_cost_of_failures = 0 # The cumulative cost of failures defined as the cost of failure per second times the number of seconds the system failed
        self._running_cost = 0 # The cumulative cost of the microservices running

    def iterate(self):
        """
            Runs the simulation for one iteration
        """

        # Run the orchestrator immediately on beginning of simulation
        # If the orchestrator period has been reached, run the orchestrator
        if self._time_since_orchestrator >= self._orchestrator_run_period:
            self.orchestrator.orchestrate(self.cloud, self._t)
            self._time_since_orchestrator = 0

        # Probabilistically update the failure or acceptance state of each container
        for microservice in self.cloud.microservices:
            for container in microservice.containers:
                if container.state == MicroserviceContainer.STATE_ACTIVE:
                    # If the container has failed, we do not ever change its state again
                    # This container has not failed, so see if it is scheduled to fail this interval
                    if container.global_to_local_time(self._t) + self._sim_clock_step >= container.local_failure_time:
                        container.state = MicroserviceContainer.STATE_FAILED
                        self._failed_containers.append(container.local_failure_time)
                        self.print_trace(f"Container {container.name} failed at local time {container.local_failure_time:.2f}.")

                    # Update the running cost of the container
                    self._running_cost += microservice.cost * self._sim_clock_step

        if self.cloud.probability_of_failure(self._t, self._sim_clock_step) >= 1:
            # The cloud has failed in this iteration
            self._actual_cost_of_failures += self.orchestrator._cost_of_failure * self._sim_clock_step

        # Update the outputs
        self._task_failure_probability.append(self.cloud.probability_of_failure(self._t, self._sim_clock_step))
        self._expected_costs_of_failure.append(self.orchestrator.expected_cost_of_failure(self._t, self._sim_clock_step, self.cloud))

        # Update the clock
        self._t += self._sim_clock_step
        self._time_since_orchestrator += self._sim_clock_step

    def finalize(self):
        """
        Adds the metrics for those containers which survived to the end to the output results
        """
        for microservice in self.cloud.microservices:
            for container in microservice.containers:
                if container.state == MicroserviceContainer.STATE_ACTIVE:
                    self._failed_containers.append(container.local_failure_time)

    def print_trace(self, msg):
        if self._trace:
            print("[SIM] " + msg)
