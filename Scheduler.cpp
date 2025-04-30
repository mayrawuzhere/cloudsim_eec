#include "Scheduler.hpp"
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <algorithm>
using namespace std;

static bool migrating = false;
static vector<MachineId_t> activeMachines;
static unordered_map<MachineId_t, unsigned> machineLoad;
static unordered_map<TaskId_t, MachineId_t> taskToMachine;
static unordered_map<MachineId_t, queue<WakeupEvent>> wakeup_maps;
static queue<TaskId_t> taskQueue;

/* functions */
void AssignTaskToMachine(TaskId_t task_id, MachineId_t machine_id, Priority_t priority);

int provisionNewMachine(CPUType_t req_cpu, VMType_t req_vm, TaskId_t task_id, Priority_t priority) {
    unsigned total = Machine_GetTotal();
    // make sure it's a new machine being used
    if (activeMachines.size() >= total) {
        SimOutput("Scheduler::Provision: No more machines available", 3);
        return -1;
    }
    for (MachineId_t id = 0; id < total; id++) {
        bool alreadyActive = std::find(activeMachines.begin(), activeMachines.end(), id) != activeMachines.end();
        if (!alreadyActive && Machine_GetCPUType(id) == req_cpu) {
            MachineInfo_t machine_info = Machine_GetInfo(id);
            if (machine_info.s_state != S0) {
                Machine_SetState(id, S0);
                SimOutput("Scheduler::Provision: Waking up machine " + to_string(id), 3);
                VMId_t vm_id = VM_Create(req_vm, req_cpu);
                wakeup_maps[id].push({id, vm_id, task_id});
                return -1;
            }
            VMId_t newVM = VM_Create(req_vm, req_cpu);
            VM_Attach(newVM, id);
            activeMachines.push_back(id);
            VM_AddTask(newVM, task_id, priority);
            machineLoad[id] = 1;
            SimOutput("Scheduler::Provision: Activated machine " + to_string(id), 3);
            return activeMachines.size() - 1;
        }
    }
    return -1;
}

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);
    activeMachines.clear();
    machineLoad.clear();
    taskToMachine.clear();
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::NewTask(): Received new task " + to_string(task_id) + " at time " + to_string(now), 3);
    Priority_t priority = (task_id == 0 || task_id == 64) ? HIGH_PRIORITY : MID_PRIORITY;
    TaskInfo_t task_info = GetTaskInfo(task_id);
    CPUType_t req_cpu = task_info.required_cpu;

    int targetMachine = -1;
    unsigned minLoad = std::numeric_limits<unsigned>::max();
    // see if we can allocate task to an existing machine
    for (unsigned i = 0; i < activeMachines.size(); i++) {
        MachineId_t machineId = activeMachines[i];
        MachineInfo_t m = Machine_GetInfo(machineId);
        if (machineLoad[activeMachines[i]] < minLoad && m.cpu == req_cpu) {
            minLoad = machineLoad[machineId];
            targetMachine = machineId;
        }
    }
    // if we can't find a suitable machine, we need to provision a new one
    if (targetMachine == -1) {
        targetMachine = provisionNewMachine(req_cpu, task_info.required_vm, task_id, priority);
        SimOutput("Scheduler::NewTask(): targetMachine " + to_string(targetMachine), 3);
        // if we still can't provision a machine, we add it to the queue
        if (targetMachine == -1) {
            taskQueue.push(task_id);
            SimOutput("Scheduler::NewTask(): Task " + to_string(task_id) + " is queued for later processing", 3);
        }
        return;
    }
    
    
    // if we found a suitable machine, we assign the task to it
    if (targetMachine == -1) {
        SimOutput("Scheduler::NewTask(): No suitable machine found for task " + to_string(task_id), 3);
        return;
    }
    AssignTaskToMachine(task_id, targetMachine, priority);
    SimOutput("Scheduler::NewTask(): Task " + to_string(task_id) + " is assigned to machine " + to_string(targetMachine), 3);
}

void AssignTaskToMachine(TaskId_t task_id, MachineId_t machine_id, Priority_t priority) {
    SimOutput("AssignTaskToMachine(): Task " + to_string(task_id) + " is assigned to machine " + to_string(machine_id), 3);
    /* machine info */
    MachineInfo_t machine_info = Machine_GetInfo(machine_id);
    /* task info */
    TaskInfo_t task_info = GetTaskInfo(task_id);
    CPUType_t req_cpu = task_info.required_cpu;
    VMType_t req_vm = task_info.required_vm;

    /* if machine doesn't have any active vms */
    if (machine_info.active_vms == 0) {
        VMId_t vm_id = VM_Create(req_vm, req_cpu);
        VM_Attach(vm_id, machine_id);
        VM_AddTask(vm_id, task_id, priority);
        SimOutput("AssignTaskToMachine(): Created new VM " + to_string(vm_id) + " on machine " + to_string(machine_id), 3);
    } else {
        /* if machine has active vms */
        for (VMId_t i = 0; i < machine_info.active_vms; i++) {
            VMInfo_t vm_info = VM_GetInfo(i);
            if (vm_info.cpu == req_cpu) {
                VM_AddTask(vm_info.vm_id, task_id, priority);
                SimOutput("AssignTaskToMachine(): Added task " + to_string(task_id) + " to VM " + to_string(vm_info.vm_id) + " on machine " + to_string(machine_id), 3);
                break;
            }
        }
    }
    /* update task to machine mapping */
    taskToMachine[task_id] = machine_id;
    machineLoad[machine_id]++;
}

void Scheduler::PeriodicCheck(Time_t now) {

}

void Scheduler::Shutdown(Time_t time) {
    for (auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
    TaskInfo_t task_info = GetTaskInfo(task_id);
    CPUType_t req_cpu = task_info.required_cpu;
    if (taskToMachine.find(task_id) != taskToMachine.end()){
         MachineId_t machineIndex = taskToMachine[task_id];
         if (machineLoad[machineIndex] > 0)
             machineLoad[machineIndex]--;
         taskToMachine.erase(task_id);
    }
    if (taskQueue.size() > 0) {
        TaskId_t nextTask = taskQueue.front();
        TaskInfo_t nextTaskInfo = GetTaskInfo(nextTask);
        CPUType_t nextReqCpu = nextTaskInfo.required_cpu;
        if (nextReqCpu != req_cpu) {
            SimOutput("Scheduler::TaskComplete(): Task " + to_string(nextTask) + " has a different CPU type than the completed task", 3);
            return;
        }
        taskQueue.pop();
        SimOutput("Scheduler::TaskComplete(): Task " + to_string(nextTask) + " is dequeued for processing", 3);
        NewTask(now, nextTask);
    }

}

static Scheduler Scheduler;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
    migrating = false;
}

void SchedulerCheck(Time_t time) {
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    SimOutput("StateChangeComplete(): Machine " + to_string(machine_id) + " is ready at time " + to_string(time), 4);
    if (wakeup_maps.find(machine_id) != wakeup_maps.end()) {
        auto& q = wakeup_maps[machine_id];
        while (!q.empty()) {
            WakeupEvent e = q.front(); q.pop();
            VM_Attach(e.vm_id, machine_id);
            VM_AddTask(e.vm_id, e.task_id, HIGH_PRIORITY);
            taskToMachine[e.task_id] = machine_id;
            machineLoad[machine_id]++; 
            SimOutput("StateChangeComplete(): Task " + to_string(e.task_id) +
                      " assigned to machine " + to_string(machine_id), 3);
        }
        wakeup_maps.erase(machine_id); // cleanup if queue is empty
    }
}
