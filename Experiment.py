from Orchestrator import *
from Cloud import *
from Microservice import *
from MicroserviceContainer import *
from Simulator import *
from SpotMarketProvider import *

import math
import statistics
import sys
import numpy
import copy


class ExponentialMicroservice(Microservice):
    def _select_random_failure_time(self):
        # Exponential distribution does not require t
        return numpy.random.exponential(1, 1)[0]

    def failure_function(self, t):
        return 1 - math.pow(math.e, -t)


class SpotMarketProvider1(SpotMarketProvider):
    def _cost_of_failure_per_second(self, t):
        hour = 24 * t/5
        if hour < 6:
            return 1
        elif hour < 7:
            return 10
        elif hour < 8:
            return 100
        elif hour < 9:
            return 1000
        elif hour < 17:
            return 100000
        elif hour < 18:
            return 1000
        elif hour < 19:
            return 100
        elif hour < 20:
            return 10
        else:
            return 1

    def _spot_price_per_second(self, t):
        """
            Returns the spot market price per second at time t
        """
        return 1

class SpotMarketProvider2(SpotMarketProvider):
    def _cost_of_failure_per_second(self, t):
        return 100000

    def _spot_price_per_second(self, t):
        """
            Returns the spot market price per second at time t
        """
        hour = 24 * t / 5
        if hour < 6:
            return 1
        elif hour < 7:
            return 10
        elif hour < 8:
            return 100
        elif hour < 9:
            return 1000
        elif hour < 17:
            return 100000
        elif hour < 18:
            return 1000
        elif hour < 19:
            return 100
        elif hour < 20:
            return 10
        else:
            return 1


class SpotMarketOrchestrator(Orchestrator):
    """
        The orchestrator's policy is to recalculate the parameters according to a SpotMarketProvider
    """

    def __init__(self, orchestrator_delta, spot_market_provider):
        """
            Creates a new orchestrator on the given cloud
            cloud - the cloud to orchestrate
            delta - the period of time for which the orchestrator will ensure reliability
            cost_of_failure - The cost of a failure
        """
        self.orchestrator_delta = orchestrator_delta
        self._spot_market_provider = spot_market_provider

    def orchestrate(self, cloud, t):
        #self.print_trace("Running orchestrator ...")
        super().orchestrate(cloud, t)
        self._ensure_at_least_one_container(cloud, t)

        cloud_before = str(cloud)
        while True:
            # Attempt to add redundant microservices until no further utility is reached
            selected_microservice = self.select_microservice_for_redundancy(cloud, t, self.orchestrator_delta)
            if selected_microservice is not None:
                new_container = cloud.microservices[selected_microservice].spawn_container(t0=t)
                #self.print_trace(f"Spawned new redundant container {cloud.microservices[selected_microservice].__class__.__name__}({cloud.microservices[selected_microservice].cost}) {new_container}.")
            else:
                #self.print_trace("No new microservices to add.")
                break

        while True:
            # Now, attempt to reduce any excess redundant microservices until no further utility is reached
            selected_microservice, selected_container = self.select_container_for_removal(cloud, t, self.orchestrator_delta)
            if selected_microservice is not None and selected_container is not None:
                removed_container = cloud.microservices[selected_microservice].remove_container(index=selected_container)
                #self.print_trace(f"Removed excess container {cloud.microservices[selected_microservice].__class__.__name__}({cloud.microservices[selected_microservice].cost}) {removed_container}.")
            else:
                #self.print_trace("No excess containers to remove.")
                break
        cloud_after = str(cloud)

        if cloud_before != cloud_after:
            self.print_trace(f"{cloud_before} -> {cloud_after}")

    def select_container_for_removal(self, cloud, t, delta):
        """
        Selects which microservice to shutdown to improve the overall running cost with respect to the failure cost
        Returns (the index into cloud.microservices, the index into cloud.microservices[i].containers)
        If no microservice should be removed, None is returned
        """
        current_expected_cost_of_failure = self.expected_cost_of_failure(t, delta, cloud)
        highest_utility = 0
        selected_microservice = None
        selected_container = None
        for i in range(len(cloud.microservices)):
            for c in range(len(cloud.microservices[i].containers)):
                proposed_cloud = copy.deepcopy(cloud)
                proposed_cloud.microservices[i].remove_container(index=c)
                proposed_expected_cost_of_failure = self.expected_cost_of_failure(t, delta, proposed_cloud)

                # Compute the utility function
                utility = current_expected_cost_of_failure - proposed_expected_cost_of_failure + cloud.microservices[i].cost * self._spot_market_provider.spot_price(t, delta)
                if utility > highest_utility:
                    highest_utility = utility
                    selected_microservice = i
                    selected_container = c

        return selected_microservice, selected_container

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
            utility = current_expected_cost_of_failure - proposed_expected_cost_of_failure - cloud.microservices[i].cost * self._spot_market_provider.spot_price(t, delta)
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
        # In the event that the cost changes during the interval (t, t+delta), we take the highest cost of failure to ensure suitable redundancy
        highest_cost = max(self._spot_market_provider.cost_of_failure(t, delta), self._spot_market_provider.cost_of_failure(t + delta, delta))
        return highest_cost * cloud.probability_of_failure(t, delta)


class ControlOrchestrator(Orchestrator):
    def __init__(self, orchestrator_delta, spot_market_provider, cost_of_failure=1):
        """
            Creates a new orchestrator on the given cloud
            cloud - the cloud to orchestrate
            delta - the period of time for which the orchestrator will ensure reliability
            cost_of_failure - The cost of a failure
        """
        self.orchestrator_delta = orchestrator_delta
        self._spot_market_provider = spot_market_provider
        self._cost_of_failure = cost_of_failure

    def orchestrate(self, cloud, t):
        self.print_trace("Running orchestrator ...")
        super().orchestrate(cloud, t)
        self._ensure_at_least_one_container(cloud, t)

        for microservice in cloud.microservices:
            while microservice.probability_of_failure(t, self.orchestrator_delta) >= 0.000000001:
                # The reliability of the microservice has dropped below the threshold level
                # Attempt to add redundant microservices
                new_container = microservice.spawn_container(t0=t)
                self.print_trace(f"Spawned new redundant container {new_container}.")

            while microservice.probability_of_failure(t, self.orchestrator_delta) < 0.000000001 and len(microservice.containers) > 1:
                removed_container = microservice.remove_container(0)
                self.print_trace(f"Removed superfluous container {removed_container}.")


class SpotMarketSimulator(Simulator):
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
                    self._running_cost += microservice.cost * self.orchestrator._spot_market_provider.spot_price(self._t, self._sim_clock_step)

        if self.cloud.probability_of_failure(self._t, self._sim_clock_step) >= 1:
            # The cloud has failed in this iteration
            self._actual_cost_of_failures += self.orchestrator._spot_market_provider.cost_of_failure(self._t, self._sim_clock_step)

        # Update the outputs
        self._task_failure_probability.append(self.cloud.probability_of_failure(self._t, self._sim_clock_step))
        self._expected_costs_of_failure.append(
            self.orchestrator.expected_cost_of_failure(self._t, self._sim_clock_step, self.cloud))

        # Update the clock
        self._t += self._sim_clock_step
        self._time_since_orchestrator += self._sim_clock_step

def spot_market_experiment(trace, cloud, orchestrator):
    simulator = SpotMarketSimulator(
        orchestrator=orchestrator,
        cloud=cloud, sim_clock_step=0.01,
        orchestrator_run_period=orchestrator.orchestrator_delta, trace=trace)
    number_of_steps = 500

    microservice_redundancy = []
    for ms in cloud.microservices:
        microservice_redundancy.append([])
    p_failure = []
    running_cost = []
    expected_failure_cost = []
    actual_failure_cost = []
    spot_price = []
    catastrophic_failure_cost = []

    for i in range(0, number_of_steps):
        previous_running_cost = simulator._running_cost
        previous_actual_failure_cost = simulator._actual_cost_of_failures

        simulator.iterate()
        p_failure.append(cloud.probability_of_failure(simulator._t, simulator._sim_clock_step))
        running_cost.append(simulator._running_cost - previous_running_cost)
        actual_failure_cost.append(simulator._actual_cost_of_failures - previous_actual_failure_cost)
        expected_failure_cost.append(cloud.probability_of_failure(simulator._t, simulator._sim_clock_step))
        spot_price.append(simulator.orchestrator._spot_market_provider._spot_price_per_second(simulator._t))
        catastrophic_failure_cost.append(simulator.orchestrator._spot_market_provider._cost_of_failure_per_second(simulator._t))

        for m in range(0, len(cloud.microservices)):
            microservice_redundancy[m].append(len(list(filter(lambda container: container.state == MicroserviceContainer.STATE_ACTIVE, cloud.microservices[m].containers))))

        #if trace:
        #    print(f"[{i}] (t={simulator._t:.2f}s): [P(failure)={p_failure}, FC={simulator.orchestrator._spot_market_provider._cost_of_failure_per_second(simulator._t)}] {str(cloud)}")

    simulator.finalize()

    if trace:
        print("Spot Market Behavior Results:")
        print(f"t,{','.join([str(x * simulator._sim_clock_step) for x in range(0, number_of_steps)])}")
        print(f"Probability of failure,{','.join([str(x) for x in p_failure])}")
        print(f"Running cost,{','.join([str(x) for x in running_cost])}")
        print(f"Expected failure cost,{','.join([str(x) for x in expected_failure_cost])}")
        print(f"Actual failure cost,{','.join([str(x) for x in actual_failure_cost])}")
        print(f"Spot price,{','.join([str(x) for x in spot_price])}")
        print(f"Catastrophic failure cost,{','.join([str(x) for x in catastrophic_failure_cost])}")

        for i in range(0, len(cloud.microservices)):
            print(f"Redundancy of MS {cloud.microservices[i].name},{','.join([str(x) for x in microservice_redundancy[i]])}")

    return simulator


def main():
    output_file = None
    if len(sys.argv) > 1:
        output_file = open(sys.argv[1], "w")
        output_file.write("Experiment,Parameter\n")

    container_failure_times = []
    running_costs = []
    actual_costs_of_failure = []
    trace = True
    num_experiments = 1

    for x in range(num_experiments):
        print(f"Experiment {x}")
        cloud = Cloud([ExponentialMicroservice(name="3-Cost MS", num_containers=1, cost=0.03), ExponentialMicroservice(name="5-Cost MS", num_containers=1, cost=.05)])
        spot_market_provider = SpotMarketProvider1()
        orchestrator = SpotMarketOrchestrator(orchestrator_delta=.01, spot_market_provider=spot_market_provider)
        #orchestrator = ControlOrchestrator(orchestrator_delta=.1, spot_market_provider=spot_market_provider)
        simulator = spot_market_experiment(trace, cloud, orchestrator)

        # Prepare the output results
        output_results = f"\n{x},RunningCost,{simulator._running_cost}"
        output_results += f"\n{x},ActualCostOfFailure,{simulator._actual_cost_of_failures}"
        output_results += "\n"
        if output_file is not None:
            output_file.write(output_results)

        if trace:
            print("Final aggregated results:")
            print(output_results)

        for res in simulator._failed_containers:
            container_failure_times.append(res)

        running_costs.append(simulator._running_cost)
        actual_costs_of_failure.append(simulator._actual_cost_of_failures)

    if len(container_failure_times) > 1:
        print(f"Container Failure Times >> Mean: {statistics.mean(container_failure_times)} | Median: {statistics.median(container_failure_times)} | Variance: {statistics.variance(container_failure_times)}")

    if len(running_costs) > 1:
        print(f"Running Cost >> Mean: {statistics.mean(running_costs)} | Median: {statistics.median(running_costs)} | Variance: {statistics.variance(running_costs)}")

    if len(actual_costs_of_failure) > 1:
        print(f"Actual Cost of Failures >> Mean: {statistics.mean(actual_costs_of_failure)} | Median: {statistics.median(actual_costs_of_failure)} | Variance: {statistics.variance(actual_costs_of_failure)}")
        total_costs = []
        for i in range(0, len(actual_costs_of_failure)):
            total_costs.append(running_costs[i] + actual_costs_of_failure[i])

        print(f"Total Cost >> Mean: {statistics.mean(total_costs)} | Median: {statistics.median(total_costs)} | Variance: {statistics.variance(total_costs)}")

    if output_file is not None:
        output_file.close()


if __name__ == '__main__':
    main()