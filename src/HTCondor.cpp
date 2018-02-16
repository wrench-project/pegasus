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

/**
     * @brief Constructor that starts the daemon for the service on a host,
     *        registering it with a WRENCH Simulation
     *
     * @param hostname: the name of the host
     * @param supports_standard_jobs: true if the job executor should support standard jobs
     * @param supports_pilot_jobs: true if the job executor should support pilot jobs
     * @param compute_resources: compute_resources: a list of <hostname, num_cores> pairs, which represent
     *        the compute resources available to this service
     * @param plist: a property list
     * @param ttl: the time-to-live, in seconds (-1: infinite time-to-live)
     * @param pj: a containing PilotJob  (nullptr if none)
     * @param suffix: a string to append to the process name
     * @param default_storage_service: a storage service
     *
     * @throw std::invalid_argument
     */
HTCondor::HTCondor(
        const std::string &hostname,
        bool supports_standard_jobs,
        bool supports_pilot_jobs,
        std::set<std::pair<std::string, unsigned long>> compute_resources,
        std::map<std::string, std::string> plist,
        double ttl,
        wrench::PilotJob *pj,
        std::string suffix,
        wrench::StorageService *default_storage_service) :
        ComputeService(hostname,
                       "multihost_multicore" + suffix,
                       "multihost_multicore" + suffix,
                       supports_standard_jobs,
                       supports_pilot_jobs,
                       default_storage_service) {

    // Set default and specified properties
    this->setProperties(this->default_property_values, plist);

    // Check that there is at least one core per host and that hosts have enough cores
    for (auto host : compute_resources) {
        std::string hname = std::get<0>(host);
        unsigned long requested_cores = std::get<1>(host);
        unsigned long available_cores = wrench::S4U_Simulation::getNumCores(hname);
        if (requested_cores == 0) {
            requested_cores = available_cores;
        }
        if (requested_cores > available_cores) {
            throw std::invalid_argument(
                    "MultihostMulticoreComputeService::MultihostMulticoreComputeService(): host " + hname + "only has " +
                    std::to_string(available_cores) + " but " +
                    std::to_string(requested_cores) + " are requested");
        }

        this->compute_resources.insert(std::make_pair(hname, requested_cores));
    }

    // Compute the total number of cores and set initial core availabilities
    this->total_num_cores = 0;
    for (auto host : this->compute_resources) {
        this->total_num_cores += std::get<1>(host);
        this->core_availabilities.insert(std::make_pair(std::get<0>(host), std::get<1>(host)));
//        this->core_availabilities[std::get<0>(host)] = std::get<1>(host);
    }

    this->ttl = ttl;
    this->has_ttl = (ttl >= 0);
    this->containing_pilot_job = pj;

//      // Start the daemon on the same host
//      try {
//        this->start_daemon(hostname);
//      } catch (std::invalid_argument &e) {
//        throw;
//      }
}


int HTCondor::main() {

    wrench::TerminalOutput::setThisProcessLoggingColor(WRENCH_LOGGING_COLOR_RED);

    WRENCH_INFO("New Multicore Job Executor starting (%s) on %ld hosts with a total of %ld cores",
                this->mailbox_name.c_str(), this->compute_resources.size(), this->total_num_cores);

    // Set an alarm for my timely death, if necessary
    if (this->has_ttl) {
        this->death_date = wrench::S4U_Simulation::getClock() + this->ttl;
        WRENCH_INFO("Will be terminating at date %lf", this->death_date);
//        std::shared_ptr<SimulationMessage> msg = std::shared_ptr<SimulationMessage>(new ServiceTTLExpiredMessage(0));
        wrench::SimulationMessage *msg = new wrench::ServiceTTLExpiredMessage(0);
        this->death_alarm = new wrench::Alarm(death_date, this->hostname, this->mailbox_name, msg, "service_string");
    } else {
        this->death_date = -1.0;
        this->death_alarm = nullptr;
    }

    /** Main loop **/
    while (this->processNextMessage()) {

        /** Dispatch jobs **/
        while (this->dispatchNextPendingJob());
    }

    wrench::WRENCH_INFO("Multicore Job Executor on host %s terminated!", wrench::S4U_Simulation::getHostName().c_str());
    return 0;
}



bool HTCondor::dispatchNextPendingJob() {

    if (this->pending_jobs.empty()) {
        return false;
    }

    wrench::WorkflowJob *picked_job = nullptr;

    std::string job_selection_policy =
            this->getPropertyValueAsString(HTCondorServiceProperty::JOB_SELECTION_POLICY);
    if (job_selection_policy == "FCFS") {
        picked_job = this->pending_jobs.front();
    } else {
        throw std::runtime_error(
                "MultihostMulticoreComputeService::dispatchNextPendingJob(): Unsupported JOB_SELECTION_POLICY '" +
                job_selection_policy + "'");
    }

    bool dispatched = false;
    switch (picked_job->getType()) {
        case wrench::WorkflowJob::STANDARD: {
            dispatched = dispatchStandardJob((wrench::StandardJob *) picked_job);
            break;
        }
        case wrench::WorkflowJob::PILOT: {
            dispatched = dispatchPilotJob((wrench::PilotJob *) picked_job);
            break;
        }
    }

    // If we dispatched, take the job out of the pending job list
    if (dispatched) {
        this->pending_jobs.pop_back();
    }
    return dispatched;
}


























