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
const Time_t IDLE_GRACE_PERIOD = 100000; // 0.1 seconds or adjust as needed
static bool offloading_in_progress = false;

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

static VMId_t PlaceTask_PMapper(TaskId_t task_id, Priority_t priority, MachineId_t original_machine);

static bool OffloadTaskFromMachine(MachineId_t m) {
    // Step 1: Gather VMs on this machine
    vector<VMId_t> candidate_vms;
    for (const auto& entry : vm_location) {
        if (entry.second == m) {
            candidate_vms.push_back(entry.first);
        }
    }

    // Step 2: For each VM, try to offload one task
    for (VMId_t vm : candidate_vms) {
        auto& tasks = vm_tasks[vm];

        for (auto it = tasks.begin(); it != tasks.end(); ) {
            TaskId_t task_to_offload = *it;
            SimOutput("Trying to offload task " + to_string(task_to_offload) + " from machine " + to_string(m), 3);

            Priority_t offload_priority = ((task_to_offload == 0 || task_to_offload == 64) ? HIGH_PRIORITY : MID_PRIORITY);
            VMId_t new_vm = PlaceTask_PMapper(task_to_offload, offload_priority, m);

            if (new_vm != INVALID_VM) {
                VM_RemoveTask(vm, task_to_offload);
                it = tasks.erase(it);
                unsigned mem = GetTaskMemory(task_to_offload);
                machine_mem_usage[m] = (machine_mem_usage[m] >= mem) ? (machine_mem_usage[m] - mem) : 0;

                SimOutput("Offloaded task " + to_string(task_to_offload) +
                          " from machine " + to_string(m) +
                          " to machine " + to_string(vm_location[new_vm]), 3);
                return true;
            } else {
                ++it;
            }
        }
    }

    return false;
}



static VMId_t PlaceTask_PMapper(TaskId_t task_id, Priority_t priority, MachineId_t original_machine) {
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
            if (info.cpu != RequiredCPUType(task_id))
                continue;
            if (m == original_machine)
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

        // ------- PROTECT THIS --------
        if (!offloading_in_progress) {
            offloading_in_progress = true;
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
                bool success = OffloadTaskFromMachine(target);
                offloading_in_progress = false;

                if (!success) {
                    break;  // no point retrying if nothing can be offloaded
                }
            } else {
                offloading_in_progress = false;
                break;
            }
        } else {
            break;  // already inside an offload → don't offload again
        }
        // ------------------------------
    }

    SimOutput("pMapper: Forced SLA violation: could not place task " + to_string(task_id), 3);
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
        MachineInfo_t info = Machine_GetInfo(m);
        VMId_t vm = VM_Create(LINUX, info.cpu);
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
    VMId_t vm = PlaceTask_PMapper(task_id, priority, -1);
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
    static unordered_map<MachineId_t, Time_t> idle_start_time;

    for (auto m : sorted_machines) {
        MachineInfo_t info = Machine_GetInfo(m);
        if (info.active_tasks > 0 || info.active_vms > 0) {
            idle_start_time.erase(m);
            continue;
        }
        

        // Start or check the idle grace period
        if (idle_start_time.find(m) == idle_start_time.end()) {
            idle_start_time[m] = now;
        } else if (now - idle_start_time[m] >= IDLE_GRACE_PERIOD) {
            MachineInfo_t info_check = Machine_GetInfo(m); // re-fetch just in case
            if (info_check.active_tasks == 0 && info_check.active_vms == 0 && info_check.s_state == S0i1) {
                //SimOutput("Machine " + to_string(m) + " has " + to_string(info_check.active_tasks) +
                //          " active tasks and " + to_string(info_check.active_vms) + " VMs and the P-STATE is " + to_string(info_check.p_state), 3);
                Machine_SetState(m, S5);
                idle_start_time.erase(m);
            } else {
                SimOutput("ABORT SHUTDOWN: Machine " + to_string(m) + " became active again!", 4);
                idle_start_time.erase(m); // Reset timer — machine isn’t idle anymore
            }
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
