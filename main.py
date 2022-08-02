import math
import statistics
import sys
import numpy
import copy

from Orchestrator import *
from Cloud import *
from Microservice import *
from MicroserviceContainer import *
from Simulator import *


class ExponentialMicroservice(Microservice):
    def random_time_to_failure_given_survival_time(self, t):
        # Exponential distribution does not require t
        return numpy.random.exponential(1, 1)[0]

    def failure_function(self, t):
        return 1 - math.pow(math.e, -t)

"""
    The orchestrator's policy is to maintain the reliability of the system above 90 percent
"""
class ExperimentalOrchestrator(Orchestrator):
    def __init__(self, orchestrator_delta, cost_of_failure=1):
        """
            Creates a new orchestrator on the given cloud
            cloud - the cloud to orchestrate
            delta - the period of time for which the orchestrator will ensure reliability
            cost_of_failure - The cost of a failure
        """
        self.orchestrator_delta = orchestrator_delta
        self._cost_of_failure = cost_of_failure

    def orchestrate(self, cloud, t):
        self.print_trace("Running orchestrator ...")
        super().orchestrate(cloud, t)

        cost_incurred = 0
        while True:
            # The reliability of the cloud has dropped below the threshold level
            # Attempt to add redundant microservices
            selected_microservice = self.select_microservice_for_redundancy(cloud, t, self.orchestrator_delta)
            if selected_microservice is not None:
                new_container = cloud.microservices[selected_microservice].spawn_container(t0=t)
                self.print_trace(f"Spawned new redundant container {new_container}.")
            else:
                break

        return cost_incurred

class ControlOrchestrator(Orchestrator):
    def __init__(self, orchestrator_delta, cost_of_failure=1):
        """
            Creates a new orchestrator on the given cloud
            cloud - the cloud to orchestrate
            delta - the period of time for which the orchestrator will ensure reliability
            cost_of_failure - The cost of a failure
        """
        self.orchestrator_delta = orchestrator_delta
        self._cost_of_failure = cost_of_failure

    def orchestrate(self, cloud, t):
        self.print_trace("Running orchestrator ...")
        super().orchestrate(cloud, t)

        cost_incurred = 0
        for microservice in cloud.microservices:
            while microservice.probability_of_failure(t, self.orchestrator_delta) >= 0.01:
                # The reliability of the microservice has dropped below the threshold level
                # Attempt to add redundant microservices
                new_container = microservice.spawn_container(t0=t)
                self.print_trace(f"Spawned new redundant container {new_container}.")
        return cost_incurred


class NOPOrchestrator(Orchestrator):
    def expected_cost_of_failure(self, t, delta, cloud):
        return 1

    def orchestrate(self, cloud, t):
        pass

def run_experiment(trace):
    cloud = Cloud([ExponentialMicroservice(num_containers=1, cost=5), ExponentialMicroservice(num_containers=1, cost=3)])
    simulator = Simulator(orchestrator=ExperimentalOrchestrator(orchestrator_delta=.01, cost_of_failure=3000), cloud=cloud, sim_clock_step=0.01,
                          orchestrator_run_period=0.01, trace=trace)

    for i in range(0, 500):
        simulator.iterate()
        p_failure = cloud.probability_of_failure(simulator._t, simulator._sim_clock_step)

        if trace:
            print(f"[{i}] (t={simulator._t:.2f}s): [P(failure)={p_failure}] {str(cloud)}")

    simulator.finalize()

    return simulator

def main():
    output_file = None
    if len(sys.argv) > 1:
        output_file = open(sys.argv[1], "w")
        output_file.write("Experiment,Parameter\n")

    container_failure_times = []
    running_costs = []
    actual_costs_of_failure = []
    trace = False

    for x in range(1000):
        print(f"Experiment {x}")
        simulator = run_experiment(trace)

        # Prepare the output results
        output_results = f"{x},TaskFailureProbability," + ",".join(str(sss) for sss in simulator._task_failure_probability)
        output_results += f"\n{x},ExpectedCostOfFailure," + ",".join(str(sss) for sss in simulator._expected_costs_of_failure)
        output_results += f"\n{x},RunningCost,{simulator._running_cost}"
        output_results += f"\n{x},ActualCostOfFailure,{simulator._actual_cost_of_failures}"
        output_results += "\n"
        if output_file is not None:
            output_file.write(output_results)

        if trace:
            print(output_results)

        for res in simulator._failed_containers:
            container_failure_times.append(res)

        running_costs.append(simulator._running_cost)
        actual_costs_of_failure.append(simulator._actual_cost_of_failures)

    print(f"Container Failure Times >> Mean: {statistics.mean(container_failure_times)} | Median: {statistics.median(container_failure_times)} | Variance: {statistics.variance(container_failure_times)}")
    print(f"Running Cost >> Mean: {statistics.mean(running_costs)} | Median: {statistics.median(running_costs)} | Variance: {statistics.variance(running_costs)}")
    print(f"Actual Cost of Failures >> Mean: {statistics.mean(actual_costs_of_failure)} | Median: {statistics.median(actual_costs_of_failure)} | Variance: {statistics.variance(actual_costs_of_failure)}")
    if output_file is not None:
        #output_file.write("\n".join([str(sss) for sss in results]))
        output_file.close()


if __name__ == '__main__':
    main()
