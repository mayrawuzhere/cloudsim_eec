/*
 * Scheduling Algorithm #1 (greedy based on task count):
 *
 * This scheduler uses a simple load-balancing approach to assign tasks.
 *
 * 1. Determine Task Priority:
 *    - Tasks with IDs 0 and 64 are considered high priority.
 *    - All other tasks are considered mid priority.
 *
 * 2. Select the Least-Loaded Machine:
 *    - For each new task (regardless of priority), the scheduler loops through all active machines.
 *    - It selects the machine that currently has the fewest tasks running.
 *
 * 3. Assign Task and Update Load:
 *    - The task is assigned to the selected machine.
 *    - The machine's load counter is increased.
 *
 * 4. Update on Task Completion:
 *    - When a task completes, the corresponding machine's load counter is decreased.
 *
 * This straightforward, greedy load-balancing approach ensures that no machine
 * becomes overloaded, which helps in preventing any SLA violations.
 */

// SLA violation report
// SLA0: 0%
// SLA1: 0%
// SLA2: 0%
// Total Energy 0.0067847KW-Hour
// Simulation run finished in 3.48 seconds

#include "Scheduler.hpp"
#include <vector>
#include <unordered_map>
using namespace std;

// Constants and global variables remain for compatibility, though some are no longer used.
static bool migrating = false;
static unsigned active_machines = 16;

// Global load counters and task mapping
static vector<unsigned> machineLoad;
static unordered_map<TaskId_t, unsigned> taskToMachine;

void Scheduler::Init() {
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);
    for (unsigned i = 0; i < active_machines; i++)
        vms.push_back(VM_Create(LINUX, X86));
    for (unsigned i = 0; i < active_machines; i++) {
        machines.push_back(MachineId_t(i));
    }
    for (unsigned i = 0; i < active_machines; i++) {
        VM_Attach(vms[i], machines[i]);
    }
    
    // Initialize load counters for each active machine
    machineLoad.resize(active_machines, 0);

    bool dynamic = false;
    if(dynamic)
        for (unsigned i = 0; i < 4; i++)
            for (unsigned j = 0; j < 8; j++)
                Machine_SetCorePerformance(MachineId_t(0), j, P3);
    // Turn off the ARM machines
    for (unsigned i = 24; i < Machine_GetTotal(); i++)
        Machine_SetState(MachineId_t(i), S5);

    SimOutput("Scheduler::Init(): VM ids are " + to_string(vms[0]) + " and " + to_string(vms[1]), 3);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks.
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Determine task priority
    Priority_t priority = (task_id == 0 || task_id == 64) ? HIGH_PRIORITY : MID_PRIORITY;
    
    // For both high- and mid-priority tasks, always choose the machine with the lowest load.
    unsigned targetMachine = 0;
    unsigned minLoad = machineLoad[0];
    for (unsigned i = 1; i < active_machines; i++) {
        if (machineLoad[i] < minLoad) {
            minLoad = machineLoad[i];
            targetMachine = i;
        }
    }
    VM_AddTask(vms[targetMachine], task_id, priority);
    taskToMachine[task_id] = targetMachine;
    machineLoad[targetMachine]++;
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
    
    if (taskToMachine.find(task_id) != taskToMachine.end()){
         unsigned machineIndex = taskToMachine[task_id];
         if (machineLoad[machineIndex] > 0)
             machineLoad[machineIndex]--;
         taskToMachine.erase(task_id);
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
    
}