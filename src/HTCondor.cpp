/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "HTCondor.h"
#include "wrench/simgrid_S4U_util/"
#include "wrench-dev.h"


XBT_LOG_NEW_DEFAULT_CATEGORY(HTCondor, "Log category for HTCondor Scheduler");

void HTCondor::submitStandardJob(wrench::StandardJob *job, std::map<std::string, std::string> &service_specific_args) {

    if (this->state == wrench::Service::DOWN) {
        throw wrench::WorkflowExecutionException(new wrench::ServiceIsDown(this));
    }

    std::string answer_mailbox = wrench::S4U_Mailbox::generateUniqueMailboxName("submit_standard_job");

    //  send a "run a standard job" message to the daemon's mailbox
    try {
        wrench::S4U_Mailbox::putMessage(this->mailbox_name,
                                new wrench::ComputeServiceSubmitStandardJobRequestMessage(
                                        answer_mailbox, job, service_specific_args,
                                        this->getPropertyValueAsDouble(
                                                wrench::ComputeServiceProperty::SUBMIT_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD)));
    } catch (std::shared_ptr<wrench::NetworkError> &cause) {
        throw wrench::WorkflowExecutionException(cause);
    }

    // Get the answer
    std::unique_ptr<wrench::SimulationMessage> message = nullptr;
    try {
        message = wrench::S4U_Mailbox::getMessage(answer_mailbox);
    } catch (std::shared_ptr<wrench::NetworkError> &cause) {
        throw wrench::WorkflowExecutionException(cause);
    }

    if (auto *msg = dynamic_cast<wrench::ComputeServiceSubmitStandardJobAnswerMessage *>(message.get())) {
        // If no success, throw an exception
        if (not msg->success) {
            throw wrench::WorkflowExecutionException(msg->failure_cause);
        }
    } else {
        throw std::runtime_error(
                "ComputeService::submitStandardJob(): Received an unexpected [" + message->getName() + "] message!");
    }
}




































//using namespace wrench;
//
//static unsigned long VM_ID = 1;
//
//    void HTCondor::scheduleTasks(JobManager *job_manager,
//                             std::map<std::string, std::vector<wrench::WorkflowTask *>> ready_tasks,
//                                 const std::set<wrench::ComputeService *> &compute_services) {
//        long scheduled = 0;
//        while(scheduled < ready_tasks.size()) {
//            for (std::set<wrench::ComputeService *>::iterator it = compute_services.begin();
//                 it != compute_services.end(); it++) {
//                WRENCH_INFO("There are %ld ready tasks to schedule", ready_tasks.size());
//                ComputeService *cs = *it;
//                std::cout << cs->process_name << std::endl;
//                for (auto itc : ready_tasks) {
//                    //TODO add support to pilot jobs
//
//                    long num_idle_cores = 0;
//
//                    // Check that it can run it right now in terms of idle cores
//                    try {
//                        num_idle_cores = cs->getNumIdleCores();
//                    } catch (WorkflowExecutionException &e) {
//                        // The service has some problem, forget it
//                        throw std::runtime_error("Unable to get the number of idle cores.");
//                    }
//
//                    // Decision making
//                    WorkflowJob *job = (WorkflowJob *) job_manager->createStandardJob(itc.second, {});
//                    //if there are no cores make more??
//                    if (num_idle_cores - scheduled <= 0) {
//                        //there are no cores do not schedule job
//                        std::cout<<"there are no cores left!!" << std::endl;
//                        if(cs->process_name == "cloud_service") {
//                            try {
//                                std::string pm_host = choosePMHostname();
//                                std::string vm_host = "vm" + std::to_string(VM_ID++) + "_" + pm_host;
//                                CloudService *cloudService = (CloudService *) (cs);
//                                if (cloudService->createVM(pm_host, vm_host,
//                                                           ((StandardJob *) (job))->getMinimumRequiredNumCores())) {
//                                    this->vm_list[pm_host].push_back(vm_host);
//                                }
//
//                            } catch (WorkflowExecutionException &e) {
//                                // unable to create a new VM, tasks won't be scheduled in this iteration.
//                                return;
//                            }
//                        }
//                    }
//                    else {
//                        job_manager->submitJob(job, cs);
//                        scheduled++;
//                    }
//                }
//            }
//        }
//    WRENCH_INFO("Done with scheduling tasks as standard jobs");
//    }
//
//
//HTCondor::HTCondor(std::vector<ComputeService *> cs, std::vector<std::string> &execution_hosts,
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
//std::string HTCondor::choosePMHostname() {
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