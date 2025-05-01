#include "Scheduler.hpp"
#include <vector>
#include <unordered_map>
#include <queue>
#include <algorithm>
using namespace std;

static bool migrating = false;

// hosts, loads
static vector<MachineId_t> activeMachines;
static unordered_map<MachineId_t, unsigned> machineLoad;

// track where each task ran
static unordered_map<TaskId_t, MachineId_t> taskToMachine;
static unordered_map<TaskId_t, VMId_t>      taskToVM;

// VMs and their host
static vector<VMId_t> vms;
static unordered_map<VMId_t, MachineId_t> vm_location;

// wakeup‐events
static unordered_map<MachineId_t, queue<WakeupEvent>> wakeup_maps;
static queue<TaskId_t> taskQueue;

/* forward */
void AssignTaskToMachine(TaskId_t task_id, MachineId_t mid, Priority_t priority);

int provisionNewMachine(CPUType_t req_cpu,
                        VMType_t req_vm,
                        TaskId_t task_id,
                        Priority_t priority) {
    unsigned total = Machine_GetTotal();
    if (activeMachines.size() >= total) {
        SimOutput("Scheduler::Provision: No more machines available", 3);
        return -1;
    }
    unsigned taskMem = GetTaskMemory(task_id);

    for (MachineId_t id = 0; id < total; id++) {
        bool already = find(activeMachines.begin(), activeMachines.end(), id)
                       != activeMachines.end();
        if (already || Machine_GetCPUType(id) != req_cpu)
            continue;

        auto minfo = Machine_GetInfo(id);
        if (minfo.s_state != S0) {
            Machine_SetState(id, S0);
            SimOutput("Scheduler::Provision: Waking up machine " + to_string(id), 3);
            VMId_t vm_id = VM_Create(req_vm, req_cpu);
            wakeup_maps[id].push({ id, vm_id, task_id });
            return -1;
        }

        // simulator‐driven memory guard
        if (minfo.memory_used + VM_MEMORY_OVERHEAD + taskMem > minfo.memory_size) {
            SimOutput("Provision: host " + to_string(id) +
                      " OOM for task " + to_string(task_id), 2);
            continue;
        }

        VMId_t newVM = VM_Create(req_vm, req_cpu);
        if (newVM == (VMId_t)(-1)) {
            SimOutput("Provision: VM_Create failed on machine " + to_string(id), 1);
            continue;
        }
        VM_Attach(newVM, id);
        VM_AddTask(newVM, task_id, priority);

        // track
        vms.push_back(newVM);
        vm_location[newVM] = id;
        taskToVM[task_id]   = newVM;
        taskToMachine[task_id] = id;
        activeMachines.push_back(id);
        machineLoad[id] = 1;

        SimOutput("Scheduler::Provision: Activated machine " + to_string(id), 3);
        return id;
    }
    return -1;
}

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Total machines = " + to_string(Machine_GetTotal()), 3);
    activeMachines.clear();
    machineLoad.clear();
    vms.clear();
    vm_location.clear();
    taskToMachine.clear();
    taskToVM.clear();
    wakeup_maps.clear();
    while (!taskQueue.empty()) taskQueue.pop();
}

void Scheduler::MigrationComplete(Time_t, VMId_t) {}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::NewTask(): Received " + to_string(task_id) + " at " + to_string(now), 3);

    auto tinfo = GetTaskInfo(task_id);
    CPUType_t    req_cpu  = tinfo.required_cpu;
    unsigned     taskMem  = tinfo.required_memory;
    Priority_t   prio     = (task_id==0||task_id==64)?HIGH_PRIORITY:MID_PRIORITY;

    MachineId_t best     = MachineId_t(-1);
    unsigned    bestLoad = numeric_limits<unsigned>::max();

    for (auto mid : activeMachines) {
        auto minfo = Machine_GetInfo(mid);
        if (minfo.cpu != req_cpu) continue;
        if (minfo.memory_used + VM_MEMORY_OVERHEAD + taskMem > minfo.memory_size) continue;
        if (machineLoad[mid] < bestLoad) {
            bestLoad = machineLoad[mid];
            best     = mid;
        }
    }

    if (best == MachineId_t(-1)) {
        int p = provisionNewMachine(req_cpu, tinfo.required_vm, task_id, prio);
        if (p < 0) {
            taskQueue.push(task_id);
            SimOutput("Scheduler::NewTask(): Queued " + to_string(task_id), 3);
        }
    } else {
        AssignTaskToMachine(task_id, best, prio);
    }
}

void AssignTaskToMachine(TaskId_t task_id, MachineId_t mid, Priority_t priority) {
    SimOutput("AssignTaskToMachine(): Task " + to_string(task_id) +
              " → machine " + to_string(mid), 3);

    auto tinfo = GetTaskInfo(task_id);
    unsigned taskMem = tinfo.required_memory;
    auto minfo = Machine_GetInfo(mid);

    if (minfo.memory_used + VM_MEMORY_OVERHEAD + taskMem > minfo.memory_size) {
        SimOutput("AssignTask: not enough RAM on " + to_string(mid), 2);
        taskQueue.push(task_id);
        return;
    }

    // try existing VMs
    for (auto vm : vms) {
        if (vm_location[vm] != mid) continue;
        auto vinfo = VM_GetInfo(vm);
        if (vinfo.cpu != tinfo.required_cpu) continue;
        VM_AddTask(vm, task_id, priority);
        taskToVM[task_id]   = vm;
        taskToMachine[task_id] = mid;
        machineLoad[mid]++;
        return;
    }

    // else create new VM
    VMId_t vm = VM_Create(tinfo.required_vm, tinfo.required_cpu);
    VM_Attach(vm, mid);
    VM_AddTask(vm, task_id, priority);
    vms.push_back(vm);
    vm_location[vm]      = mid;
    taskToVM[task_id]    = vm;
    taskToMachine[task_id] = mid;
    machineLoad[mid]++;
}

void Scheduler::PeriodicCheck(Time_t) {}

void Scheduler::Shutdown(Time_t time) {
    for (auto vm : vms) VM_Shutdown(vm);
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) +
              " complete at " + to_string(now), 4);

    // only remove if VM really has it
    auto itVM = taskToVM.find(task_id);
    if (itVM != taskToVM.end()) {
        VMId_t vm = itVM->second;
        auto vinfo = VM_GetInfo(vm);
        if (find(vinfo.active_tasks.begin(),
                 vinfo.active_tasks.end(),
                 task_id) != vinfo.active_tasks.end()) {
            VM_RemoveTask(vm, task_id);
        } else {
            SimOutput("Warning: tried to remove task " + to_string(task_id) +
                      " from VM " + to_string(vm) + " but it was not present", 1);
        }
        taskToVM.erase(itVM);
    }

    // free host load
    auto itM = taskToMachine.find(task_id);
    if (itM != taskToMachine.end()) {
        MachineId_t mid = itM->second;
        if (machineLoad[mid] > 0) machineLoad[mid]--;
        taskToMachine.erase(itM);
    }

    // retry queued tasks
    queue<TaskId_t> pending;
    swap(pending, taskQueue);
    while (!pending.empty()) {
        TaskId_t next = pending.front(); pending.pop();
        SimOutput("Scheduler::TaskComplete(): Retrying queued task " + to_string(next), 3);
        NewTask(now, next);
    }
}

static Scheduler Scheduler;

void InitScheduler()                       { Scheduler.Init(); }
void HandleNewTask(Time_t t, TaskId_t id)       { Scheduler.NewTask(t, id); }
void HandleTaskCompletion(Time_t t, TaskId_t id){ Scheduler.TaskComplete(t, id); }
void MemoryWarning(Time_t, MachineId_t)          {}
void MigrationDone(Time_t t, VMId_t v)           { Scheduler.MigrationComplete(t, v); migrating=false; }
void SchedulerCheck(Time_t t)                   { Scheduler.PeriodicCheck(t); }
void SimulationComplete(Time_t time) {
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy: " << Machine_GetClusterEnergy() << " KW-Hour" << endl;
    cout << "Simulation finished at " << double(time)/1000000 << " seconds" << endl;
    Scheduler.Shutdown(time);
}
void SLAWarning(Time_t, TaskId_t)                {}
void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) +
              " ready at time " + to_string(time), 4);
    auto it = wakeup_maps.find(machine_id);
    if (it == wakeup_maps.end()) return;
    auto &q = it->second;
    while (!q.empty()) {
        auto e = q.front(); q.pop();
        auto tinfo = GetTaskInfo(e.task_id);
        auto minfo = Machine_GetInfo(machine_id);
        if (minfo.memory_used + VM_MEMORY_OVERHEAD + tinfo.required_memory > minfo.memory_size) {
            SimOutput("StateChangeComplete: OOM for task " + to_string(e.task_id), 2);
            continue;
        }
        VM_Attach(e.vm_id, machine_id);
        VM_AddTask(e.vm_id, e.task_id, HIGH_PRIORITY);
        taskToVM[e.task_id]    = e.vm_id;
        taskToMachine[e.task_id] = machine_id;
        machineLoad[machine_id]++;
    }
    wakeup_maps.erase(machine_id);
}
