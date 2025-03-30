//  pMapper Scheduler Algorithm:
//    - Sorts machines by energy consumption.
//    - Places tasks on machines based on available memory and task capacity.
//    - Offloads tasks from overloaded machines to prevent memory overcommitment.
//    - Prioritizes high-priority tasks (e.g., tasks 0 and 64) to meet SLA requirements.

#include "Scheduler.hpp"
#include <algorithm>
#include <vector>
#include <unordered_map>
#include <limits>
#include <string>

using namespace std;

static const VMId_t INVALID_VM = (VMId_t)(-1);

static bool migrating = false;
static unsigned active_machines = 16;

static vector<MachineId_t> sorted_machines;
static vector<unsigned> machine_util;
static vector<unsigned> machine_mem_usage;
static vector<unsigned> machine_mem_capacity;

static vector<VMId_t> vms;
static unordered_map<VMId_t, MachineId_t> vm_location;
static unordered_map<VMId_t, vector<TaskId_t>> vm_tasks;

static const unsigned MAX_TASKS_PER_VM = 10;

static double GetMachineEnergy(MachineId_t m) {
    return static_cast<double>(Machine_GetEnergy(m));
}

static void SortMachinesByEnergy() {
    sorted_machines.clear();
    unsigned total = Machine_GetTotal();
    for (unsigned i = 0; i < total; i++) {
        sorted_machines.push_back(i);
    }
    sort(sorted_machines.begin(), sorted_machines.end(),
         [](MachineId_t a, MachineId_t b) {
             return GetMachineEnergy(a) < GetMachineEnergy(b);
         });
}

static bool MachineHasCapacity(MachineId_t m) {
    return machine_util[m] < MAX_TASKS_PER_VM;
}

static VMId_t PlaceTask_PMapper(TaskId_t task_id, Priority_t priority);

static void OffloadTaskFromMachine(MachineId_t m) {
    for (auto &entry : vm_location) {
        if (entry.second == m) {
            VMId_t vm = entry.first;
            vector<TaskId_t>& tasks = vm_tasks[vm];
            if (!tasks.empty()) {
                TaskId_t task_to_offload = tasks.front();
                tasks.erase(tasks.begin());
                unsigned mem = GetTaskMemory(task_to_offload);
                if (machine_mem_usage[m] >= mem)
                    machine_mem_usage[m] -= mem;
                else
                    machine_mem_usage[m] = 0;
                VM_RemoveTask(vm, task_to_offload);
                Priority_t offload_priority = ((task_to_offload == 0 || task_to_offload == 64) ? HIGH_PRIORITY : MID_PRIORITY);
                VMId_t new_vm = PlaceTask_PMapper(task_to_offload, offload_priority);
                if (new_vm != INVALID_VM) {
                    SimOutput("Offloaded task " + to_string(task_to_offload) +
                              " from machine " + to_string(m) +
                              " to machine " + to_string(vm_location[new_vm]), 2);
                } else {
                    SimOutput("Offload failed for task " + to_string(task_to_offload), 0);
                }
                break;
            }
        }
    }
}

static VMId_t PlaceTask_PMapper(TaskId_t task_id, Priority_t priority) {
    unsigned taskMem = GetTaskMemory(task_id);
    const unsigned max_attempts = 3;
    for (unsigned attempt = 0; attempt < max_attempts; attempt++) {
        for (auto m : sorted_machines) {
            MachineInfo_t info = Machine_GetInfo(m);
            if (info.s_state == S5) {
                Machine_SetState(m, S0);
            }
            if (machine_mem_usage[m] + taskMem > machine_mem_capacity[m])
                continue;
            if (MachineHasCapacity(m)) {
                for (auto vm : vms) {
                    if (vm_location[vm] == m && vm_tasks[vm].size() < MAX_TASKS_PER_VM) {
                        VM_AddTask(vm, task_id, priority);
                        vm_tasks[vm].push_back(task_id);
                        machine_util[m]++;
                        machine_mem_usage[m] += taskMem;
                        return vm;
                    }
                }
                VMId_t new_vm = VM_Create(RequiredVMType(task_id), RequiredCPUType(task_id));
                VM_Attach(new_vm, m);
                vm_location[new_vm] = m;
                vms.push_back(new_vm);
                vm_tasks[new_vm] = vector<TaskId_t>();
                VM_AddTask(new_vm, task_id, priority);
                vm_tasks[new_vm].push_back(task_id);
                machine_util[m]++;
                machine_mem_usage[m] += taskMem;
                return new_vm;
            }
        }
        double worst_ratio = 0.0;
        MachineId_t target = (MachineId_t)(-1);
        for (auto m : sorted_machines) {
            double ratio = double(machine_mem_usage[m]) / (machine_mem_capacity[m] ? machine_mem_capacity[m] : 1);
            if (ratio > worst_ratio && ratio < 1.0) {
                worst_ratio = ratio;
                target = m;
            }
        }
        if (target != (MachineId_t)(-1)) {
            OffloadTaskFromMachine(target);
        }
    }
    SimOutput("pMapper: Forced SLA violation: could not place task " + to_string(task_id), 0);
    return INVALID_VM;
}


void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Initializing pMapper scheduler", 1);
    
    SortMachinesByEnergy();
    unsigned total = Machine_GetTotal();
    
    machine_util.resize(total, 0);
    machine_mem_usage.resize(total, 0);
    machine_mem_capacity.resize(total, 0);
    
    for (unsigned m = 0; m < total; m++) {
        MachineInfo_t info = Machine_GetInfo(m);
        machine_mem_capacity[m] = info.memory_size;
        machine_mem_usage[m] = info.memory_used;
    }
    
    for (unsigned i = 0; i < active_machines && i < sorted_machines.size(); i++) {
        MachineId_t m = sorted_machines[i];
        Machine_SetState(m, S0);
        VMId_t vm = VM_Create(LINUX, X86);
        VM_Attach(vm, m);
        vm_location[vm] = m;
        vms.push_back(vm);
        vm_tasks[vm] = vector<TaskId_t>();
    }
    SimOutput("Scheduler::Init(): pMapper initialization complete", 2);
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    Priority_t priority = (task_id == 0 || task_id == 64) ? HIGH_PRIORITY : MID_PRIORITY;
    SimOutput("Scheduler::NewTask(): Received task " + to_string(task_id) +
              " at time " + to_string(now), 3);
    VMId_t vm = PlaceTask_PMapper(task_id, priority);
    if (vm == INVALID_VM) {
        SimOutput("Scheduler::NewTask(): SLA violation for task " + to_string(task_id), 0);
    }
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) +
              " completed at time " + to_string(now), 3);
    for (auto &entry : vm_tasks) {
        vector<TaskId_t>& tasks = entry.second;
        auto it = find(tasks.begin(), tasks.end(), task_id);
        if (it != tasks.end()) {
            tasks.erase(it);
            MachineId_t m = vm_location[entry.first];
            if (machine_util[m] > 0)
                machine_util[m]--;
            unsigned mem = GetTaskMemory(task_id);
            if (machine_mem_usage[m] >= mem)
                machine_mem_usage[m] -= mem;
            else
                machine_mem_usage[m] = 0;
            break;
        }
    }
}

void Scheduler::PeriodicCheck(Time_t now) {
    SimOutput("Scheduler::PeriodicCheck(): Checking system at time " + to_string(now), 4);
    for (auto m : sorted_machines) {
        if (machine_util[m] == 0) {
            Machine_SetState(m, S5);
        }
    }
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    migrating = false;
    MachineId_t new_machine = VM_GetInfo(vm_id).machine_id;
    vm_location[vm_id] = new_machine;
    SimOutput("Scheduler::MigrationComplete(): Migration of VM " + to_string(vm_id) +
              " complete at time " + to_string(time), 2);
}

void Scheduler::Shutdown(Time_t time) {
    for (auto &vm : vms) {
        VM_Shutdown(vm);
    }
    SimOutput("Scheduler::Shutdown(): All VMs shut down at time " + to_string(time), 4);
}

static Scheduler Scheduler;

void InitScheduler() {
    SimOutput("InitScheduler(): Starting pMapper scheduler", 4);
    Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    SimOutput("MemoryWarning(): Memory overflow on machine " + to_string(machine_id) +
              " at time " + to_string(time), 0);
    OffloadTaskFromMachine(machine_id);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    Scheduler.MigrationComplete(time, vm_id);
}

void SchedulerCheck(Time_t time) {
    Scheduler.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy: " << Machine_GetClusterEnergy() << " KW-Hour" << endl;
    cout << "Simulation run finished at " << double(time) / 1000000 << " seconds" << endl;
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    SimOutput("SLAWarning(): Warning for task " + to_string(task_id) +
              " at time " + to_string(time), 0);
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {}