/*
 * Scheduling Algorithm #2 (Dynamic High-Power Provisioning with CPU Compatibility):
 *
 * This scheduler aims to complete tasks as fast as possible by:
 *
 * 1. Dynamically provisioning machines that are compatible with the task’s requirements:
 *    - On a new task arrival, the scheduler obtains the task's required CPU type and VM type.
 *    - It then searches for an inactive machine with a matching CPU type.
 *    - If found, it creates a new VM using the task’s required VM type and attaches it to that machine.
 *
 * 2. Highest Performance Configuration:
 *    - Every newly activated machine is configured so that all its cores run at the highest performance state (P0).
 *
 * 3. Task Assignment:
 *    - If a new machine is not available, the scheduler searches among the active machines that match the task’s CPU type.
 *    - It first tries to find an idle machine (zero load), and if none is found, it selects the one with the lowest load.
 *
 * 4. Shutdown Procedure:
 *    - At simulation end, all active machines are first set to the lowest power state (S5)
 *      and then their VMs are shut down.
 *
 * This approach maximizes concurrency by running tasks only on machines that meet the task’s CPU requirements.
 */

#include "Scheduler.hpp"
#include "Interfaces.h"
#include <vector>
#include <unordered_map>
#include <string>
#include <limits>
using namespace std;

// Global data structures for dynamic provisioning.
static vector<VMId_t> activeVMs;              // VMs created for active machines.
static vector<MachineId_t> activeMachines;    // Machine IDs that have been powered on.
static vector<unsigned> machineLoad;          // Number of tasks running per active machine.
static unordered_map<TaskId_t, unsigned> taskToMachine; // Maps task ID to index in activeMachines.

// Constants for the scheduler.
const unsigned NUM_CORES = 8;               // Assume 8 cores per machine.
const CPUPerformance_t HIGHEST_PERF = P0;     // P0 is the highest performance state.
const MachineState_t LOWEST_POWER = S5;       // S5 is the powered-down state.

// Provision a new machine that matches the required CPU type.
// Returns the index in activeMachines if successful, or -1 if no matching inactive machine exists.
int provisionNewMachine(CPUType_t req_cpu, VMType_t req_vm) {
    unsigned total = Machine_GetTotal();
    // Look for a machine that is not yet active and matches the required CPU type.
    for (MachineId_t id = 0; id < total; id++) {
        bool alreadyActive = false;
        for (MachineId_t activeId : activeMachines) {
            if (activeId == id) {
                alreadyActive = true;
                break;
            }
        }
        if (!alreadyActive) {
            // Only provision if the machine's CPU type matches the task's requirement.
            if (Machine_GetCPUType(id) != req_cpu)
                continue;
            // Create a new VM using the task's required VM type and CPU type.
            VMId_t newVM = VM_Create(req_vm, req_cpu);
            VM_Attach(newVM, id);
            // Set all cores to the highest performance state.
            for (unsigned core = 0; core < NUM_CORES; core++) {
                Machine_SetCorePerformance(id, core, HIGHEST_PERF);
            }
            // Add this machine and its VM to our active lists.
            activeMachines.push_back(id);
            activeVMs.push_back(newVM);
            machineLoad.push_back(0);
            SimOutput("Scheduler::Provision: Activated machine " + to_string(id) +
                      " with CPU type " + to_string(req_cpu) +
                      " using VM type " + (req_vm == LINUX ? "LINUX" : "LINUX_RT") +
                      " at highest performance (P0)", 3);
            return activeMachines.size() - 1;
        }
    }
    return -1;  // No matching inactive machine available.
}

// Find an active machine that matches the required CPU type and is idle (load 0).
// If none is idle, returns the index of the machine with the smallest load among matching machines.
// Returns -1 if no active machine with the required CPU exists.
int findCompatibleMachine(CPUType_t req_cpu) {
    int bestIndex = -1;
    unsigned minLoad = std::numeric_limits<unsigned>::max();
    for (unsigned i = 0; i < activeMachines.size(); i++) {
        if (Machine_GetCPUType(activeMachines[i]) == req_cpu) {
            // Prefer an idle machine.
            if (machineLoad[i] == 0)
                return i;
            if (machineLoad[i] < minLoad) {
                minLoad = machineLoad[i];
                bestIndex = i;
            }
        }
    }
    return bestIndex;
}

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Initializing dynamic high-power scheduler", 1);
    activeVMs.clear();
    activeMachines.clear();
    machineLoad.clear();
    taskToMachine.clear();
    // Machines will be activated on-demand.
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::NewTask(): Received task " + to_string(task_id) +
              " at time " + to_string(now), 4);
    // Use the task's requirements.
    CPUType_t task_cpu = RequiredCPUType(task_id);
    VMType_t task_vm = RequiredVMType(task_id);
    // Always assign tasks with HIGH_PRIORITY for fastest execution.
    Priority_t priority = HIGH_PRIORITY;
    
    // Try to provision a new machine that matches the task's CPU type.
    int targetIndex = provisionNewMachine(task_cpu, task_vm);
    
    if (targetIndex == -1) {
        // No matching inactive machine exists; search among active machines.
        targetIndex = findCompatibleMachine(task_cpu);
        if (targetIndex == -1) {
            // This should not happen if there is at least one active machine with the required type.
            ThrowException("No active machine available with required CPU type for task " + to_string(task_id));
            return;
        }
        SimOutput("Scheduler::NewTask(): Reusing active machine " +
                  to_string(activeMachines[targetIndex]), 3);
    }
    
    // Assign the task to the selected machine's VM.
    VM_AddTask(activeVMs[targetIndex], task_id, priority);
    taskToMachine[task_id] = targetIndex;
    machineLoad[targetIndex]++;
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) +
              " completed at time " + to_string(now), 4);
    if (taskToMachine.find(task_id) != taskToMachine.end()) {
        unsigned machineIndex = taskToMachine[task_id];
        if (machineLoad[machineIndex] > 0)
            machineLoad[machineIndex]--;
        taskToMachine.erase(task_id);
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Not used in this algorithm.
}

void Scheduler::PeriodicCheck(Time_t now) {
    // Optionally implement periodic monitoring.
}

void Scheduler::Shutdown(Time_t time) {
    // Before shutdown, set all active machines to the lowest power state.
    for (unsigned i = 0; i < activeMachines.size(); i++) {
        Machine_SetState(activeMachines[i], LOWEST_POWER);
        SimOutput("Scheduler::Shutdown(): Machine " + to_string(activeMachines[i]) +
                  " set to lowest power state (S5)", 3);
    }
    // Shut down all associated VMs.
    for (VMId_t vm : activeVMs) {
        VM_Shutdown(vm);
    }
    SimOutput("Scheduler::Shutdown(): Shutdown complete at time " + to_string(time), 4);
}

// Instantiate the Scheduler.
static Scheduler SchedulerInstance;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    SchedulerInstance.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received task " + to_string(task_id) +
              " at time " + to_string(time), 4);
    SchedulerInstance.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) +
              " completed at time " + to_string(time), 4);
    SchedulerInstance.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    SimOutput("MemoryWarning(): Memory warning on machine " + to_string(machine_id) +
              " at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) +
              " completed at time " + to_string(time), 4);
    SchedulerInstance.MigrationComplete(time, vm_id);
}

void SchedulerCheck(Time_t time) {
    SimOutput("SchedulerCheck(): Periodic check at time " + to_string(time), 4);
    SchedulerInstance.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time) / 1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    
    SchedulerInstance.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    // Not used in this implementation.
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Not used in this implementation.
}