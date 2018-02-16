//
// Created by james oeth on 1/25/18.
//

#include "Schedd.h"


XBT_LOG_NEW_DEFAULT_CATEGORY(Schedd, "Log category for Schedd Scheduler");


using namespace wrench;

static unsigned long VM_ID = 1;

void Schedd::scheduleTasks(JobManager *job_manager,
                             std::map<std::string, std::vector<wrench::WorkflowTask *>> ready_tasks,
                             const std::set<wrench::ComputeService *> &compute_services) {
    WRENCH_INFO("There are %ld ready tasks to schedule", ready_tasks.size());
    for (auto itc : ready_tasks) {
        bool successfully_scheduled = false;
        // Second: attempt to run the task on a compute resource
        WRENCH_INFO("Trying to submit task '%s' to a standard compute service...", itc.first.c_str());

        for (auto cs : compute_services) {
            WRENCH_INFO("Asking compute service %s if it can run this standard job...", cs->getName().c_str());

            // Check that the compute service could in principle run this job
            if ((not cs->isUp()) || (not cs->supportsStandardJobs())) {
                continue;
            }

            // Get the number of idle cores
            unsigned long num_idle_cores;

            // Check that it can run it right now in terms of idle cores
            try {
                num_idle_cores = cs->getNumIdleCores();
            } catch (WorkflowExecutionException &e) {
                // The service has some problem, forget it
                continue;
            }

            // Decision making
            if (num_idle_cores <= 0) {
                continue;
            }

            // We can submit!
            WRENCH_INFO("Submitting task %s for execution as a standard job", itc.first.c_str());
            WorkflowJob *job = (WorkflowJob *) job_manager->createStandardJob(itc.second, {});
            job_manager->submitJob(job, cs);
            successfully_scheduled = true;
            break;
        }

        if (not successfully_scheduled) {
            WRENCH_INFO("no dice");
            break;
        }

    }
    WRENCH_INFO("Done with scheduling tasks as standard jobs");
}


//Schedd::Schedd(std::vector<ComputeService *> cs, std::vector<std::string> &execution_hosts,
//                   wrench::Simulation *simulation) {
//    //TODO: figure out how to check new service
//    if (execution_hosts.empty()) {
//        throw std::runtime_error("At least one execution host should be provided");
//    }
//    //TODO: check to make sure simulations is correct
//    this->simulation = simulation;
//    this->execution_hosts = execution_hosts;
//    this->computeService = cs;
//}
//
///**
//    * @brief Select a physical host (PM) with the least number of VMs.
//    *
//    * @return a physical hostname
//    */
//std::string Schedd::choosePMHostname() {
//
//    std::pair<std::string, unsigned long> min_pm("", ULONG_MAX);
//
//    for (auto &host : this->execution_hosts) {
//        auto entry = this->vm_list.find(host);
//
//        if (entry == this->vm_list.end()) {
//            return host;
//        }
//        if (entry->second.size() < min_pm.second) {
//            min_pm.first = entry->first;
//            min_pm.second = entry->second.size();
//        }
//    }
//
//    return min_pm.first;
//}