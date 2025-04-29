#include "Scheduler.hpp"
#include "Interfaces.h"
#include <vector>
#include <unordered_map>
#include <string>
#include <limits>
#include <unordered_set>
#include <queue>
using namespace std;

static vector<VMId_t> activeVMs;
static vector<MachineId_t> activeMachines;
static vector<unsigned> machineLoad;
static vector<Time_t> machineIdleStart;
static unordered_map<TaskId_t, unsigned> taskToMachine;
static vector<pair<VMId_t, MachineId_t>> pending_vm_attachments;
static std::unordered_set<MachineId_t> waking_up_machines;
static std::queue<TaskId_t> deferred_tasks;

const unsigned NUM_CORES = 8;
const CPUPerformance_t HIGHEST_PERF = P0;
const MachineState_t LOWEST_POWER = S5;
const Time_t IDLE_THRESHOLD = 200000;
const MachineId_t INVALID_MACHINE = static_cast<MachineId_t>(-1);

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
        
        if (!alreadyActive && Machine_GetCPUType(id) == req_cpu) {
            MachineInfo_t machine_info = Machine_GetInfo(id);
            if (machine_info.s_state != S0) {
                Machine_SetState(id, S0);
                waking_up_machines.insert(id);
                SimOutput("Scheduler::Provision: Waking up machine " + to_string(id), 3);
                VMId_t vm_id = VM_Create(req_vm, req_cpu);
                pending_vm_attachments.push_back(make_pair(vm_id, id));
                return -1;
            }
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

int findCompatibleMachine(CPUType_t req_cpu) {
    int bestIndex = -1;
    unsigned minLoad = std::numeric_limits<unsigned>::max();
    for (unsigned i = 0; i < activeMachines.size(); i++) {
        if (Machine_GetCPUType(activeMachines[i]) == req_cpu) {
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
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);
    activeVMs.clear();
    activeMachines.clear();
    machineLoad.clear();
    machineIdleStart.clear();
    taskToMachine.clear();
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::NewTask(): Received task " + to_string(task_id) + " at time " + to_string(now), 3);
    CPUType_t task_cpu = RequiredCPUType(task_id);
    VMType_t task_vm = RequiredVMType(task_id);
    Priority_t priority = HIGH_PRIORITY;
    int targetIndex = -1;

    targetIndex = provisionNewMachine(task_cpu, task_vm);
    if (targetIndex == -1) {
        targetIndex = findCompatibleMachine(task_cpu);
        SimOutput("Scheduler::NewTask(): Reusing active machine " + to_string(activeMachines[targetIndex]), 3);
    }
    if (targetIndex == -1) {
        SimOutput("Scheduler::NewTask(): No suitable machine found for task " + to_string(task_id), 3);
        deferred_tasks.push(task_id);
        return;
    }
   
    
    VM_AddTask(activeVMs[targetIndex], task_id, priority);
    
    SimOutput("Scheduler::NewTask(): Add task " + to_string(task_id) + " to machine " + to_string(activeVMs[targetIndex]), 3);
    taskToMachine[task_id] = targetIndex;
    machineLoad[targetIndex]++;
    machineIdleStart[targetIndex] = 0;
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " completed at time " + to_string(now), 3);
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
        MachineId_t machine_id = activeMachines[i];
        MachineInfo_t machine_info = Machine_GetInfo(machine_id); 
        if (waking_up_machines.count(machine_id))
            continue; // 🚀 Machine is waking up, DO NOT shut down yet! 
        if (machine_info.active_tasks == 0 && machine_info.active_vms == 0 && 
                machineIdleStart[i] != 0 && (now - machineIdleStart[i] >= IDLE_THRESHOLD)) {
            SimOutput("Scheduler::PeriodicCheck(): Shutting down idle machine " + to_string(activeMachines[i]), 3);
            VMInfo_t vm_info = VM_GetInfo(activeVMs[i]);
            if (vm_info.active_tasks.size() > 0) {
                SimOutput("Scheduler::PeriodicCheck(): VM " + to_string(activeVMs[i]) + " still has active tasks", 3);
                continue;
            }
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
    SimOutput("Scheduler::Shutdown(): Shutdown complete at time " + to_string(time), 3);
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

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) + " is now ON", 3);
    for (auto it = pending_vm_attachments.begin(); it != pending_vm_attachments.end(); ++it) {
        auto [vm_id, id] = *it;
        if (id == machine_id) {
            try {
                VM_Attach(vm_id, machine_id);
                for (unsigned core = 0; core < NUM_CORES; core++) {
                    Machine_SetCorePerformance(machine_id, core, HIGHEST_PERF);
                }
                activeMachines.push_back(machine_id);
                activeVMs.push_back(vm_id);
                machineLoad.push_back(0);
                machineIdleStart.push_back(0);
                waking_up_machines.erase(machine_id);
                SimOutput("Scheduler::StateChangeComplete: Attached VM " + to_string(vm_id) + " to machine " + to_string(machine_id), 3);
                pending_vm_attachments.erase(it);
                // Attempt to assign any deferred tasks now that the machine is ready
                std::queue<TaskId_t> remaining_tasks;
                while (!deferred_tasks.empty()) {
                    TaskId_t task_id = deferred_tasks.front();
                    deferred_tasks.pop();

                    CPUType_t cpu = RequiredCPUType(task_id);
                    unsigned mem = GetTaskMemory(task_id);

                    if (Machine_GetCPUType(machine_id) == cpu) {
                        MachineInfo_t info = Machine_GetInfo(machine_id);
                        unsigned availableMem = info.memory_size - info.memory_used;
                        if (availableMem >= mem) {
                            VM_AddTask(vm_id, task_id, HIGH_PRIORITY);
                            taskToMachine[task_id] = activeMachines.size() - 1;
                            machineLoad.back()++;
                            machineIdleStart.back() = 0;
                            SimOutput("Scheduler::StateChangeComplete(): Assigned deferred task " + to_string(task_id), 3);
                            continue;
                        }
                    }
                    remaining_tasks.push(task_id); // Still can't be placed
                }
                deferred_tasks = remaining_tasks;
            } catch (...) {
                SimOutput("StateChangeComplete(): Attach failed for VM " + to_string(vm_id), 3);
            }
            break;
        }
    }
}
