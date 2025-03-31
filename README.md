# Cloud Simulation Scheduling Algorithms

This repository contains four scheduling algorithms used in a cloud simulation environment. Each algorithm is designed to efficiently manage task assignments, balance system load, and optimize energy usage. The branches corresponding to each algorithm are also noted.

---

## Scheduling Algorithm #1: Greedy Based on Task Count  
**Branch:** `main`

### Approach

- **Task Priority Determination:**
  - Tasks with IDs **0** and **64** are marked as high priority.
  - All other tasks are considered mid priority.
  - Assignment is based solely on the current load of each machine.

- **Machine Selection:**
  - Iterates over all active machines.
  - Selects the machine with the fewest running tasks.

- **Task Assignment & Load Tracking:**
  - The selected machine receives the new task.
  - Its load counter is incremented.
  - A mapping between the task and machine is maintained.
  - Upon task completion, the load is updated accordingly.

### Benefits

- **Simple Strategy:**  
  - Easy-to-implement approach.
- **Effective Load Balancing:**  
  - Helps prevent SLA violations by evenly distributing tasks.

---

## Scheduling Algorithm #2: Reactive Provisioning with Idle Consolidation  
**Branch:** `algo-2`

### Approach

- **Task Arrival & Provisioning:**
  - Determines the required CPU and VM types when a task arrives.
  - Searches for an active machine that is idle or lightly loaded.
  - If none is found, provisions a new machine by creating and attaching a new VM on an inactive machine that meets the CPU requirement.

- **Dynamic Load Tracking & Idle Shutdown:**
  - Active machines are monitored for load.
  - When a machine becomes idle, its idle time is recorded.
  - Periodic checks shut down machines that have been idle longer than a preset threshold, transitioning them to a low-power state.

- **Shutdown Procedure:**
  - At the end of the simulation, all remaining active machines are transitioned to the lowest power state and shut down.

### Benefits

- **Energy Efficiency:**  
  - Enhances energy efficiency by shutting down idle machines.
- **Dynamic Resource Provisioning:**  
  - Scales resources to meet demand as needed.

---

## Scheduling Algorithm #3: Adaptive Multi-Criteria Load Balancing with Idle Consolidation  
**Branch:** `algo-3`

### Approach

- **Task Arrival & Resource Matching:**
  - For each new task, determines the required CPU, VM, and memory.
  - **High-Priority Tasks (SLA0):**
    - Trigger immediate provisioning of a new machine.
    - If provisioning fails, selects a compatible active machine.
  - **Other Tasks:**
    - Searches active machines using multiple criteria (load and available memory).
    - Provisions a new machine only if necessary.

- **Dynamic Monitoring & Idle Shutdown:**
  - Monitors machine loads and idle times.
  - Machines remaining idle beyond a defined threshold are shut down to conserve energy.

- **Shutdown Procedure:**
  - At simulation completion, all active machines are powered down.

### Benefits

- **Optimized Task Placement:**  
  - Considers multiple resource constraints (CPU, memory, load).
- **Quick Handling of High-Priority Tasks:**  
  - Prioritizes rapid provisioning for high-priority tasks.
- **Energy Savings:**  
  - Consolidates idle resources to reduce energy usage.

---

## Scheduling Algorithm #4: pMapper Scheduler Algorithm  
**Branch:** `algo-4-literature`

### Approach

- **Energy-Aware Machine Sorting:**
  - Machines are sorted by their energy consumption to prioritize lower-energy options.

- **Task Placement Based on Memory and Capacity:**
  - Checks available memory.
  - Ensures that the number of tasks does not exceed a defined capacity (maximum tasks per VM).

- **Dynamic Offloading:**
  - If a machine is at risk of memory overcommitment or is overloaded, offloads a task (typically the oldest task in the queue) to another machine.
  - Offloading is attempted for a fixed number of retries.

- **High-Priority Task Handling:**
  - High-priority tasks (e.g., tasks **0** and **64**) are given precedence to meet SLA requirements.

- **Periodic Checks and Shutdown:**
  - Machines not in use are switched to a low-power state during periodic checks.
  - During shutdown, all VMs are terminated.

### Benefits

- **Energy Efficiency:**  
  - Leverages low-energy machines.
- **Resource Management:**  
  - Considers both memory usage and task capacity to prevent overcommitment.
- **Dynamic Load Balancing:**  
  - Offloads tasks to balance load and maintain SLA compliance.

---

## How LLM's Were Used

- **Debugging:**
  - Identified and resolved algorithm implementation errors.
- **Explanation:**
  - Provided detailed explanations of the code and overall simulator structure.
- **Code Fixes:**
  - Assisted in fixing code issues.
- **Formatting & README Creation:**
  - Helped format the code and create this comprehensive README.
