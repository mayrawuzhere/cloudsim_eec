/*
 * Scheduling Algorithm #3 (Adaptive Multi-Criteria Load Balancing with Idle Consolidation):
 *
 * This scheduler aims to complete tasks quickly while saving energy by:
 *
 * 1. On task arrival:
 *    - Determine the task’s required CPU, VM, and memory.
 *    - For high-priority (SLA0) tasks, attempt to provision a new machine immediately.
 *      For other tasks, first search active machines using multi-criteria matching.
 *    - If no active machine is eligible, provision a new machine on an inactive machine
 *      that meets the task’s CPU requirement.
 *
 * 2. While running:
 *    - Track each machine’s load and record idle start times when load drops to zero.
 *    - Periodically, if a machine remains idle longer than a threshold,
 *      shut it down (transition to lowest power state and shut down its VM).
 *
 * 3. At shutdown:
 *    - Set all remaining active machines to the lowest power state and shut down their VMs.
 */

#include "Scheduler.hpp"
#include "Interfaces.h"
#include <vector>
#include <unordered_map>
#include <string>
#include <limits>
using namespace std;

static vector<VMId_t> activeVMs;
static vector<MachineId_t> activeMachines;
static vector<unsigned> machineLoad;
static vector<Time_t> machineIdleStart;
static unordered_map<TaskId_t, unsigned> taskToMachine;

const unsigned NUM_CORES = 8;
const CPUPerformance_t HIGHEST_PERF = P0;
const MachineState_t LOWEST_POWER = S5;
const Time_t IDLE_THRESHOLD = 200000;  // microseconds

int provisionNewMachine(CPUType_t req_cpu, VMType_t req_vm) {
    unsigned total = Machine_GetTotal();
    for (MachineId_t id = 0; id < total; id++) {
        bool alreadyActive = false;
        for (MachineId_t activeId : activeMachines) {
            if (activeId == id) {
                alreadyActive = true;
                break;
            }
        }
        if (!alreadyActive) {
            if (Machine_GetCPUType(id) != req_cpu)
                continue;
            VMId_t newVM = VM_Create(req_vm, req_cpu);
            VM_Attach(newVM, id);
            for (unsigned core = 0; core < NUM_CORES; core++) {
                Machine_SetCorePerformance(id, core, HIGHEST_PERF);
            }
            activeMachines.push_back(id);
            activeVMs.push_back(newVM);
            machineLoad.push_back(0);
            machineIdleStart.push_back(0);
            SimOutput("Scheduler::Provision: Activated machine " + to_string(id), 3);
            return activeMachines.size() - 1;
        }
    }
    return -1;
}

int findCompatibleMachine(CPUType_t req_cpu, unsigned req_mem) {
    int bestIndex = -1;
    unsigned bestLoad = std::numeric_limits<unsigned>::max();
    unsigned bestAvailMem = 0;
    for (unsigned i = 0; i < activeMachines.size(); i++) {
        if (Machine_GetCPUType(activeMachines[i]) != req_cpu)
            continue;
        MachineInfo_t info = Machine_GetInfo(activeMachines[i]);
        unsigned availableMem = (info.memory_size > info.memory_used) ? (info.memory_size - info.memory_used) : 0;
        if (availableMem < req_mem)
            continue;
        if (machineLoad[i] == 0)
            return i;
        if (machineLoad[i] < bestLoad ||
           (machineLoad[i] == bestLoad && availableMem > bestAvailMem)) {
            bestLoad = machineLoad[i];
            bestAvailMem = availableMem;
            bestIndex = i;
        }
    }
    return bestIndex;
}

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);
    activeVMs.clear();
    activeMachines.clear();
    machineLoad.clear();
    machineIdleStart.clear();
    taskToMachine.clear();
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::NewTask(): Received task " + to_string(task_id) +
              " at time " + to_string(now), 4);
    CPUType_t task_cpu = RequiredCPUType(task_id);
    VMType_t task_vm = RequiredVMType(task_id);
    unsigned taskMem = GetTaskMemory(task_id);
    SLAType_t taskSLA = RequiredSLA(task_id);
    Priority_t priority = HIGH_PRIORITY;
    
    int targetIndex = -1;
    // For high priority (SLA0), try to provision a new machine first.
    if (taskSLA == SLA0) {
        targetIndex = provisionNewMachine(task_cpu, task_vm);
        if (targetIndex == -1) {
            targetIndex = findCompatibleMachine(task_cpu, taskMem);
        }
        SimOutput("Scheduler::NewTask(): High priority task " + to_string(task_id) +
                  " assigned to machine " + to_string(activeMachines[targetIndex]), 3);
    } else {
        targetIndex = findCompatibleMachine(task_cpu, taskMem);
        if (targetIndex == -1) {
            targetIndex = provisionNewMachine(task_cpu, task_vm);
        } else {
            SimOutput("Scheduler::NewTask(): Reusing active machine " +
                      to_string(activeMachines[targetIndex]), 3);
        }
    }
    if (targetIndex == -1) {
        ThrowException("No machine available with required CPU type for task " + to_string(task_id));
        return;
    }
    VM_AddTask(activeVMs[targetIndex], task_id, priority);
    taskToMachine[task_id] = targetIndex;
    machineLoad[targetIndex]++;
    machineIdleStart[targetIndex] = 0;
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) +
              " completed at time " + to_string(now), 4);
    if (taskToMachine.find(task_id) != taskToMachine.end()) {
        unsigned machineIndex = taskToMachine[task_id];
        if (machineLoad[machineIndex] > 0)
            machineLoad[machineIndex]--;
        taskToMachine.erase(task_id);
        if (machineLoad[machineIndex] == 0)
            machineIdleStart[machineIndex] = now;
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    for (int i = activeMachines.size() - 1; i >= 0; i--) {
        if (machineLoad[i] == 0 && machineIdleStart[i] != 0 && (now - machineIdleStart[i] >= IDLE_THRESHOLD)) {
            SimOutput("Scheduler::PeriodicCheck(): Shutting down idle machine " + to_string(activeMachines[i]), 3);
            Machine_SetState(activeMachines[i], LOWEST_POWER);
            VM_Shutdown(activeVMs[i]);
            activeMachines.erase(activeMachines.begin() + i);
            activeVMs.erase(activeVMs.begin() + i);
            machineLoad.erase(machineLoad.begin() + i);
            machineIdleStart.erase(machineIdleStart.begin() + i);
        }
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) { }

void Scheduler::Shutdown(Time_t time) {
    for (unsigned i = 0; i < activeMachines.size(); i++) {
        Machine_SetState(activeMachines[i], LOWEST_POWER);
        VM_Shutdown(activeVMs[i]);
    }
    SimOutput("Scheduler::Shutdown(): Shutdown complete at time " + to_string(time), 4);
}

static Scheduler SchedulerInstance;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    SchedulerInstance.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received task " + to_string(task_id) + " at time " + to_string(time), 4);
    SchedulerInstance.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    SchedulerInstance.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    SimOutput("MemoryWarning(): Memory warning on machine " + to_string(machine_id) + " at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " completed at time " + to_string(time), 4);
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

void SLAWarning(Time_t time, TaskId_t task_id) { }

void StateChangeComplete(Time_t time, MachineId_t machine_id) { }
