This repository contains four scheduling algorithms used in a cloud simulation environment. Each algorithm is designed to efficiently manage task assignments, balance system load, and optimize energy usage. Below is an outline of the approaches used in each algorithm.

Scheduling Algorithm #1: Greedy Based on Task Count (branch: main)
Approach:

Task Priority Determination:
Tasks with IDs 0 and 64 are marked as high priority, while all others are considered mid priority. The assignment is based solely on the current load of each machine.

Machine Selection:
The scheduler iterates over all active machines and selects the one with the fewest running tasks.

Task Assignment & Load Tracking:
The selected machine receives the new task, its load counter is incremented, and a mapping between the task and machine is maintained. On task completion, the load is updated accordingly.

Benefits:

Simple, easy-to-implement strategy

Effective load balancing that helps prevent SLA violations

Scheduling Algorithm #2: Reactive Provisioning with Idle Consolidation (branch: algo-2)
Approach:

Task Arrival & Provisioning:
When a task arrives, its required CPU and VM types are determined. The scheduler searches for an active machine that is idle or lightly loaded. If none is found, it provisions a new machine by creating and attaching a new VM on an inactive machine that meets the CPU requirement.

Dynamic Load Tracking & Idle Shutdown:
Active machines are monitored for load. When a machine becomes idle, its idle time is recorded. Periodic checks shut down machines that have been idle longer than a preset threshold, transitioning them to a low-power state.

Shutdown Procedure:
At the end of the simulation, all remaining active machines are transitioned to the lowest power state and shut down.

Benefits:

Enhances energy efficiency by shutting down idle machines

Dynamically provisions resources to scale with demand

Scheduling Algorithm #3: Adaptive Multi-Criteria Load Balancing with Idle Consolidation (branch: algo-3)
Approach:

Task Arrival & Resource Matching:
For each new task, the scheduler determines the required CPU, VM, and memory.

High-Priority Tasks (SLA0): Trigger immediate provisioning of a new machine. If provisioning fails, a compatible active machine is selected.

Other Tasks: The scheduler searches active machines using multiple criteria (load and available memory) and provisions a new machine only if necessary.

Dynamic Monitoring & Idle Shutdown:
Machine loads and idle times are monitored. Machines remaining idle beyond a defined threshold are shut down to conserve energy.

Shutdown Procedure:
At simulation completion, all active machines are powered down.

Benefits:

Optimizes task placement by considering multiple resource constraints

Prioritizes quick handling of high-priority tasks while consolidating idle resources for energy savings

Scheduling Algorithm #4: pMapper Scheduler Algorithm (branch: algo-4-literature)
Approach:

Energy-Aware Machine Sorting:
Machines are sorted by their energy consumption to prioritize lower-energy options.

Task Placement Based on Memory and Capacity:
Tasks are placed on machines by checking available memory and ensuring that the number of tasks does not exceed a defined capacity (maximum tasks per VM).

Dynamic Offloading:
If a machine is at risk of memory overcommitment or is overloaded, the scheduler offloads a task (typically the oldest task in the queue) to another machine. The offloading is attempted for a fixed number of retries.

High-Priority Task Handling:
High-priority tasks (such as tasks 0 and 64) are given precedence during placement to meet SLA requirements.

Periodic Checks and Shutdown:
In periodic checks, machines that are not utilized are switched to a low-power state. During shutdown, all VMs are terminated.

Benefits:

Prioritizes energy efficiency by leveraging low-energy machines

Considers both memory usage and task capacity to prevent overcommitment

Dynamically offloads tasks to balance load and maintain SLA compliance

How LLM's were used:
1) Debugging algorithm implementation errors and providing logging.
2) Explaining beginning code and overall simulator structure.
3) Providing code fixes.
4) Formatting and README creation.