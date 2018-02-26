/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "HTCondor.h"
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

    WRENCH_INFO("Multicore Job Executor on host %s terminated!", wrench::S4U_Simulation::getHostName().c_str());
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


bool HTCondor::processNextMessage() {

    // Wait for a message
    std::unique_ptr<wrench::SimulationMessage> message;

    try {
        message = wrench::S4U_Mailbox::getMessage(this->mailbox_name);
    } catch (std::shared_ptr<wrench::NetworkError> &cause) {
        WRENCH_INFO("Got a network error while getting some message... ignoring");
        return true;
    }

    if (message == nullptr) {
        WRENCH_INFO("Got a NULL message... Likely this means we're all done. Aborting");
        return false;
    }

    WRENCH_INFO("Got a [%s] message", message->getName().c_str());

    if (auto *msg = dynamic_cast<wrench::ServiceTTLExpiredMessage *>(message.get())) {
        WRENCH_INFO("My TTL has expired, terminating and perhaps notify a pilot job submitted");
        this->terminate(true);
        return false;

    } else if (auto *msg = dynamic_cast<wrench::ServiceStopDaemonMessage *>(message.get())) {
        this->terminate(false);
        // This is Synchronous
        try {
            wrench::S4U_Mailbox::putMessage(msg->ack_mailbox,
                                    new wrench::ServiceDaemonStoppedMessage(this->getPropertyValueAsDouble(
                                            wrench::MultihostMulticoreComputeServiceProperty::DAEMON_STOPPED_MESSAGE_PAYLOAD)));
        } catch (std::shared_ptr<wrench::NetworkError> &cause) {
            return false;
        }
        return false;

    } else if (auto *msg = dynamic_cast<wrench::ComputeServiceSubmitStandardJobRequestMessage *>(message.get())) {
        processSubmitStandardJob(msg->answer_mailbox, msg->job, msg->service_specific_args);
        return true;

    } else if (auto *msg = dynamic_cast<wrench::ComputeServiceSubmitPilotJobRequestMessage *>(message.get())) {
        processSubmitPilotJob(msg->answer_mailbox, msg->job);
        return true;

    } else if (auto *msg = dynamic_cast<wrench::ComputeServicePilotJobExpiredMessage *>(message.get())) {
        processPilotJobCompletion(msg->job);
        return true;

    } else if (auto *msg = dynamic_cast<wrench::ComputeServiceResourceInformationRequestMessage *>(message.get())) {

        processGetResourceInformation(msg->answer_mailbox);
        return true;

    } else if (auto *msg = dynamic_cast<wrench::ComputeServiceTerminateStandardJobRequestMessage *>(message.get())) {

        processStandardJobTerminationRequest(msg->job, msg->answer_mailbox);
        return true;

    } else if (auto *msg = dynamic_cast<wrench::ComputeServiceTerminatePilotJobRequestMessage *>(message.get())) {

        processPilotJobTerminationRequest(msg->job, msg->answer_mailbox);
        return true;

    }
    //TODO: find out how to access these maybe new pull/install
    //else if (auto *msg = dynamic_cast<wrench::StandardJobExecutorDoneMessage *>(message.get())) {
//        processStandardJobCompletion(msg->executor, msg->job);
//        return true;
//
//    } else if (auto *msg = dynamic_cast<wrench::StandardJobExecutorFailedMessage *>(message.get())) {
//        processStandardJobFailure(msg->executor, msg->job, msg->cause);
//        return true;

    //}
    else {
        throw std::runtime_error("Unexpected [" + message->getName() + "] message");
    }
}


/**
     * @brief Process a submit standard job request
     *
     * @param answer_mailbox: the mailbox to which the answer message should be sent
     * @param job: the job
     * @param service_specific_args: service specific arguments
     *
     * @throw std::runtime_error
     */
void HTCondor::processSubmitStandardJob(
        const std::string &answer_mailbox, wrench::StandardJob *job,
        std::map<std::string, std::string> &service_specific_arguments) {
    WRENCH_INFO("Asked to run a standard job with %ld tasks", job->getNumTasks());

    // Do we support standard jobs?
    if (not this->supportsStandardJobs()) {
        try {
            wrench::S4U_Mailbox::dputMessage(
                    answer_mailbox,
                    new wrench::ComputeServiceSubmitStandardJobAnswerMessage(
                            job, this, false, std::shared_ptr<wrench::FailureCause>(new wrench::JobTypeNotSupported(job, this)),
                            this->getPropertyValueAsDouble(
                                    wrench::ComputeServiceProperty::SUBMIT_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD)));
        } catch (std::shared_ptr<wrench::NetworkError> &cause) {
            return;
        }
        return;
    }

    // Can we run this job assuming the whole set of resources is available?
    unsigned long max_min_num_cores = 0;
    for (auto t : job->getTasks()) {
        max_min_num_cores = MAX(max_min_num_cores, t->getMinNumCores());
    }
    bool enough_resources = false;
    for (auto r : this->compute_resources) {
        unsigned long num_cores = r.second;
        if (num_cores >= max_min_num_cores) {
            enough_resources = true;
        }
    }

    if (!enough_resources) {
        try {
            wrench::S4U_Mailbox::dputMessage(
                    answer_mailbox,
                    new wrench::ComputeServiceSubmitStandardJobAnswerMessage(
                            job, this, false, std::shared_ptr<wrench::FailureCause>(new wrench::NotEnoughComputeResources(job, this)),
                            this->getPropertyValueAsDouble(
                                    wrench::MultihostMulticoreComputeServiceProperty::NOT_ENOUGH_CORES_MESSAGE_PAYLOAD)));
        } catch (std::shared_ptr<wrench::NetworkError> &cause) {
            return;
        }
        return;
    }

    // Since we can run, add the job to the list of pending jobs
    this->pending_jobs.push_front((wrench::WorkflowJob *) job);

    try {
        wrench::S4U_Mailbox::dputMessage(
                answer_mailbox,
                new wrench::ComputeServiceSubmitStandardJobAnswerMessage(
                        job, this, true, nullptr, this->getPropertyValueAsDouble(
                                wrench::ComputeServiceProperty::SUBMIT_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD)));
    } catch (std::shared_ptr<wrench::NetworkError> &cause) {
        return;
    }
}

/**
    * @brief Terminate the daemon, dealing with pending/running jobs
    *
    * @param notify_pilot_job_submitters:
    */
void HTCondor::terminate(bool notify_pilot_job_submitters) {

    this->setStateToDown();

    WRENCH_INFO("Failing current standard jobs");
    this->failCurrentStandardJobs(std::shared_ptr<wrench::FailureCause>(new wrench::ServiceIsDown(this)));

    WRENCH_INFO("Terminate all pilot jobs");
    this->terminateAllPilotJobs();

    // Am I myself a pilot job?
    if (notify_pilot_job_submitters && this->containing_pilot_job) {

        WRENCH_INFO("Letting the level above know that the pilot job has ended on mailbox %s",
                    this->containing_pilot_job->getCallbackMailbox().c_str());
        // NOTE: This is synchronous so that the process doesn't fall off the end
        try {
            wrench::S4U_Mailbox::putMessage(this->containing_pilot_job->popCallbackMailbox(),
                                    new wrench::ComputeServicePilotJobExpiredMessage(
                                            this->containing_pilot_job, this,
                                            this->getPropertyValueAsDouble(
                                                    wrench::MultihostMulticoreComputeServiceProperty::PILOT_JOB_EXPIRED_MESSAGE_PAYLOAD)));
        } catch (std::shared_ptr<wrench::NetworkError> &cause) {
            return;
        }
    }
}


/**
     * @brief Terminate all pilot job compute services
     */
void HTCondor::terminateAllPilotJobs() {
    for (auto job : this->running_jobs) {
        if (job->getType() == wrench::WorkflowJob::PILOT) {
            auto *pj = (wrench::PilotJob *) job;
            WRENCH_INFO("Terminating pilot job %s", job->getName().c_str());
            pj->getComputeService()->stop();
        }
    }
}

/**
   * @brief Declare all current jobs as failed (likely because the daemon is being terminated
   * or has timed out (because it's in fact a pilot job))
   */
void HTCondor::failCurrentStandardJobs(std::shared_ptr<wrench::FailureCause> cause) {

    for (auto workflow_job : this->running_jobs) {
        if (workflow_job->getType() == wrench::WorkflowJob::STANDARD) {
            wrench::StandardJob *job = (wrench::StandardJob *) workflow_job;
            this->failRunningStandardJob(job, std::move(cause));
        }
    }

    while (not this->pending_jobs.empty()) {
        wrench::WorkflowJob *workflow_job = this->pending_jobs.front();
        this->pending_jobs.pop_back();
        if (workflow_job->getType() == wrench::WorkflowJob::STANDARD) {
            auto *job = (wrench::StandardJob *) workflow_job;
            this->failPendingStandardJob(job, cause);
        }
    }
}


/**
    * @brief Process a standard job completion
    * @param executor: the standard job executor
    * @param job: the job
    *
    * @throw std::runtime_error
    */
void HTCondor::processStandardJobCompletion(wrench::StandardJobExecutor *executor, wrench::StandardJob *job) {

    // Update core availabilities
    for (auto r : executor->getComputeResources()) {
        this->core_availabilities[r.first] += r.second;

    }

    // Remove the executor from the executor list
    bool found_it = false;
    for (auto it = this->standard_job_executors.begin();
         it != this->standard_job_executors.end(); it++) {
        if ((*it).get() == executor) {

            wrench::PointerUtil::moveUniquePtrFromSetToSet(it, &(this->standard_job_executors), &(this->completed_job_executors));

            found_it = true;
            break;
        }
    }

    if (!found_it) {
        throw std::runtime_error(
                "MultihostMulticoreComputeService::processStandardJobCompletion(): Received a standard job completion, but the executor is not in the executor list");
    }

    // Remove the job from the running job list
    if (this->running_jobs.find(job) == this->running_jobs.end()) {
        throw std::runtime_error(
                "MultihostMulticoreComputeService::processStandardJobCompletion(): Received a standard job completion, but the job is not in the running job list");
    }
    this->running_jobs.erase(job);

    WRENCH_INFO("A standard job executor has completed job %s", job->getName().c_str());


    // Send the callback to the originator
    try {
        wrench::S4U_Mailbox::dputMessage(
                job->popCallbackMailbox(), new wrench::ComputeServiceStandardJobDoneMessage(
                        job, this, this->getPropertyValueAsDouble(
                                wrench::MultihostMulticoreComputeServiceProperty::STANDARD_JOB_DONE_MESSAGE_PAYLOAD)));
    } catch (std::shared_ptr<wrench::NetworkError> &cause) {
        return;
    }
}


/**
     * @brief Process a work failure
     * @param worker_thread: the worker thread that did the work
     * @param work: the work
     * @param cause: the cause of the failure
     */
void HTCondor::processStandardJobFailure(wrench::StandardJobExecutor *executor,
                                         wrench::StandardJob *job,
                                                                 std::shared_ptr<wrench::FailureCause> cause) {

    // Update core availabilities
    for (auto r : executor->getComputeResources()) {
        this->core_availabilities[r.first] += r.second;

    }

    // Remove the executor from the executor list
    bool found_it = false;
    for (auto it = this->standard_job_executors.begin();
         it != this->standard_job_executors.end(); it++) {
        if ((*it).get() == executor) {
            wrench::PointerUtil::moveUniquePtrFromSetToSet(it, &(this->standard_job_executors), &(this->completed_job_executors));
            found_it = true;
            break;
        }
    }

    // Remove the executor from the executor list
    if (!found_it) {
        throw std::runtime_error(
                "MultihostMulticoreComputeService::processStandardJobCompletion(): Received a standard job completion, but the executor is not in the executor list");
    }

    // Remove the job from the running job list
    if (this->running_jobs.find(job) == this->running_jobs.end()) {
        throw std::runtime_error(
                "MultihostMulticoreComputeService::processStandardJobCompletion(): Received a standard job completion, but the job is not in the running job list");
    }
    this->running_jobs.erase(job);

    WRENCH_INFO("A standard job executor has failed to perform job %s", job->getName().c_str());

    // Fail the job
    this->failPendingStandardJob(job, cause);

}

/**
    * @brief Process a pilot job completion
    *
    * @param job: the pilot job
    */
void HTCondor::processPilotJobCompletion(wrench::PilotJob *job) {

    // Remove the job from the running list
    this->running_jobs.erase(job);

    auto *cs = (wrench::MultihostMulticoreComputeService *) job->getComputeService();

    // Update core availabilities
    for (auto r : cs->compute_resources) {
        std::string hostname = std::get<0>(r);
        unsigned long num_cores = std::get<1>(r);

        this->core_availabilities[hostname] += num_cores;
    }

    // Forward the notification
    try {
        wrench::S4U_Mailbox::dputMessage(job->popCallbackMailbox(), new wrench::ComputeServicePilotJobExpiredMessage(
                job, this, this->getPropertyValueAsDouble(
                        wrench::MultihostMulticoreComputeServiceProperty::PILOT_JOB_EXPIRED_MESSAGE_PAYLOAD)));
    } catch (std::shared_ptr<wrench::NetworkError> &cause) {
        return;
    }
}

/**
    * @brief Synchronously terminate a standard job previously submitted to the compute service
    *
    * @param job: the standard job
    *
    * @throw WorkflowExecutionException
    * @throw std::runtime_error
    */
void HTCondor::terminateStandardJob(wrench::StandardJob *job) {

    if (this->state == wrench::Service::DOWN) {
        throw wrench::WorkflowExecutionException(new wrench::ServiceIsDown(this));
    }

    std::string answer_mailbox = wrench::S4U_Mailbox::generateUniqueMailboxName("terminate_standard_job");

    //  send a "terminate a standard job" message to the daemon's mailbox
    try {
        wrench::S4U_Mailbox::putMessage(this->mailbox_name,
                                new wrench::ComputeServiceTerminateStandardJobRequestMessage(
                                        answer_mailbox, job, this->getPropertyValueAsDouble(
                                                wrench::MultihostMulticoreComputeServiceProperty::TERMINATE_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD)));
    } catch (std::shared_ptr<wrench::NetworkError> &cause) {
        throw wrench::WorkflowExecutionException(cause);
    }

    // Get the answer
    std::unique_ptr<wrench::SimulationMessage> message = nullptr;
    try {
        message = wrench::S4U_Mailbox::getMessage(answer_mailbox);
    } catch (std::shared_ptr<wrench::NetworkError> &cause) {
        throw WorkflowExecutionException(cause);
    }

    if (auto *msg = dynamic_cast<wrench::ComputeServiceTerminateStandardJobAnswerMessage *>(message.get())) {
        // If no success, throw an exception
        if (not msg->success) {
            throw wrench::WorkflowExecutionException(msg->failure_cause);
        }
    } else {
        throw std::runtime_error(
                "MultihostMulticoreComputeService::terminateStandardJob(): Received an unexpected [" +
                message->getName() + "] message!");
    }
}

/**
     * @brief Synchronously terminate a pilot job to the compute service
     *
     * @param job: a pilot job
     *
     * @throw WorkflowExecutionException
     * @throw std::runtime_error
     */
void HTCondor::terminatePilotJob(wrench::PilotJob *job) {

    if (this->state == wrench::Service::DOWN) {
        throw wrench::WorkflowExecutionException(new wrench::ServiceIsDown(this));
    }

    std::string answer_mailbox = wrench::S4U_Mailbox::generateUniqueMailboxName("terminate_pilot_job");

    // Send a "terminate a pilot job" message to the daemon's mailbox
    try {
        wrench::S4U_Mailbox::putMessage(this->mailbox_name,
                                new wrench::ComputeServiceTerminatePilotJobRequestMessage(
                                        answer_mailbox, job, this->getPropertyValueAsDouble(
                                                wrench::MultihostMulticoreComputeServiceProperty::TERMINATE_PILOT_JOB_REQUEST_MESSAGE_PAYLOAD)));
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

    if (auto *msg = dynamic_cast<wrench::ComputeServiceTerminatePilotJobAnswerMessage *>(message.get())) {
        // If no success, throw an exception
        if (not msg->success) {
            throw wrench::WorkflowExecutionException(msg->failure_cause);
        }

    } else {
        throw std::runtime_error(
                "MultihostMulticoreComputeService::terminatePilotJob(): Received an unexpected ["
                + message->getName() + "] message!");
    }
}

/**
     * @brief Process a standard job termination request
     *
     * @param job: the job to terminate
     * @param answer_mailbox: the mailbox to which the answer message should be sent
     */
void HTCondor::processStandardJobTerminationRequest(wrench::StandardJob *job, std::string answer_mailbox) {

    // Check whether job is pending
    for (auto it = this->pending_jobs.begin(); it < this->pending_jobs.end(); it++) {
        if (*it == job) {
            this->pending_jobs.erase(it);
            wrench::ComputeServiceTerminateStandardJobAnswerMessage *answer_message = new wrench::ComputeServiceTerminateStandardJobAnswerMessage(
                    job, this, true, nullptr,
                    this->getPropertyValueAsDouble(
                            wrench::MultihostMulticoreComputeServiceProperty::TERMINATE_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD));
            try {
                wrench::S4U_Mailbox::dputMessage(answer_mailbox, answer_message);
            } catch (std::shared_ptr<wrench::NetworkError> &cause) {
                return;
            }
            return;
        }
    }

    // Check whether the job is running
    if (this->running_jobs.find(job) != this->running_jobs.end()) {
        // Remove the job from the list of running jobs
        this->running_jobs.erase(job);
        // terminate it
        terminateRunningStandardJob(job);
        // reply
        wrench::ComputeServiceTerminateStandardJobAnswerMessage *answer_message = new wrench::ComputeServiceTerminateStandardJobAnswerMessage(
                job, this, true, nullptr,
                this->getPropertyValueAsDouble(
                        wrench::MultihostMulticoreComputeServiceProperty::TERMINATE_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD));
        try {
            wrench::S4U_Mailbox::dputMessage(answer_mailbox, answer_message);
        } catch (std::shared_ptr<wrench::NetworkError> &cause) {
            return;
        }
        return;
    }

    // If we got here, we're in trouble
    WRENCH_INFO("Trying to terminate a standard job that's neither pending nor running!");
    wrench::ComputeServiceTerminateStandardJobAnswerMessage *answer_message = new wrench::ComputeServiceTerminateStandardJobAnswerMessage(
            job, this, false, std::shared_ptr<wrench::FailureCause>(new wrench::JobCannotBeTerminated(job)),
            this->getPropertyValueAsDouble(
                    wrench::MultihostMulticoreComputeServiceProperty::TERMINATE_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD));
    try {
        wrench::S4U_Mailbox::dputMessage(answer_mailbox, answer_message);
    } catch (std::shared_ptr<wrench::NetworkError> &cause) {
        return;
    }
}


/**
    * @brief Process a pilot job termination request
    *
    * @param job: the job to terminate
    * @param answer_mailbox: the mailbox to which the answer message should be sent
    */
void HTCondor::processPilotJobTerminationRequest(wrench::PilotJob *job, std::string answer_mailbox) {

    // Check whether job is pending
    for (auto it = this->pending_jobs.begin(); it < this->pending_jobs.end(); it++) {
        if (*it == job) {
            this->pending_jobs.erase(it);
            wrench::ComputeServiceTerminatePilotJobAnswerMessage *answer_message = new wrench::ComputeServiceTerminatePilotJobAnswerMessage(
                    job, this, true, nullptr,
                    this->getPropertyValueAsDouble(
                            wrench::MultihostMulticoreComputeServiceProperty::TERMINATE_PILOT_JOB_ANSWER_MESSAGE_PAYLOAD));
            try {
                wrench::S4U_Mailbox::dputMessage(answer_mailbox, answer_message);
            } catch (std::shared_ptr<wrench::NetworkError> &cause) {
                return;
            }
            return;
        }
    }

    // Check whether the job is running
    if (this->running_jobs.find(job) != this->running_jobs.end()) {
        this->running_jobs.erase(job);
        terminateRunningPilotJob(job);
        wrench::ComputeServiceTerminatePilotJobAnswerMessage *answer_message = new wrench::ComputeServiceTerminatePilotJobAnswerMessage(
                job, this, true, nullptr,
                this->getPropertyValueAsDouble(
                        wrench::MultihostMulticoreComputeServiceProperty::TERMINATE_PILOT_JOB_ANSWER_MESSAGE_PAYLOAD));
        try {
            wrench::S4U_Mailbox::dputMessage(answer_mailbox, answer_message);
        } catch (std::shared_ptr<wrench::NetworkError> &cause) {
            return;
        }
        return;
    }

    // If we got here, we're in trouble
    WRENCH_INFO("Trying to terminate a pilot job that's neither pending nor running!");
    wrench::ComputeServiceTerminatePilotJobAnswerMessage *answer_message = new wrench::ComputeServiceTerminatePilotJobAnswerMessage(
            job, this, false, std::shared_ptr<wrench::FailureCause>(new wrench::JobCannotBeTerminated(job)),
            this->getPropertyValueAsDouble(
                    wrench::MultihostMulticoreComputeServiceProperty::TERMINATE_PILOT_JOB_ANSWER_MESSAGE_PAYLOAD));
    try {
        wrench::S4U_Mailbox::dputMessage(answer_mailbox, answer_message);
    } catch (std::shared_ptr<wrench::NetworkError> &cause) {
        return;
    }
}


/**
    * @brief Try to dispatch a standard job
    * @param job: the job
    * @return true is the job was dispatched, false otherwise
    */
bool HTCondor::dispatchStandardJob(wrench::StandardJob *job) {

    // Compute the required minimum number of cores
    unsigned long minimum_required_num_cores = 1;

//      WRENCH_INFO("IN DISPATCH");
//      for (auto r : this->core_availabilities) {
//        WRENCH_INFO("   --> %s %ld", std::get<0>(r).c_str(), std::get<1>(r));
//      }

    for (auto t : (job)->getTasks()) {
        minimum_required_num_cores = MAX(minimum_required_num_cores, t->getMinNumCores());
    }

    // Find the list of hosts with the required number of cores
    std::set<std::string> possible_hosts;
    for (auto it = this->core_availabilities.begin(); it != this->core_availabilities.end(); it++) {
        WRENCH_INFO("%s: %ld", it->first.c_str(), it->second);
        if (it->second >= minimum_required_num_cores) {
            possible_hosts.insert(it->first);
        }
    }

    // If not even one host, give up
    if (possible_hosts.empty()) {
//      WRENCH_INFO("*** THERE ARE NOT ENOUGH RESOURCES FOR THIS JOB!!");
        return false;
    }


    // Allocate resources for the job based on resource allocation strategies
    std::set<std::pair<std::string, unsigned long>> compute_resources;
    compute_resources = wrench::computeResourceAllocation(job);

    // Update core availabilities (and compute total number of cores for printing)
    unsigned long total_cores = 0;
    for (auto r : compute_resources) {
        this->core_availabilities[std::get<0>(r)] -= std::get<1>(r);
        total_cores += std::get<1>(r);
    }

    WRENCH_INFO("Creating a StandardJobExecutor on %ld hosts (%ld cores in total) for a standard job",
                compute_resources.size(), total_cores);
    // Create a standard job executor
    wrench::StandardJobExecutor *executor = new wrench::StandardJobExecutor(
            this->simulation,
            this->mailbox_name,
            this->hostname,
            job,
            compute_resources,
            this->default_storage_service,
            {{wrench::StandardJobExecutorProperty::THREAD_STARTUP_OVERHEAD,   this->getPropertyValueAsString(
                    wrench::MultihostMulticoreComputeServiceProperty::THREAD_STARTUP_OVERHEAD)},
             {wrench::StandardJobExecutorProperty::CORE_ALLOCATION_ALGORITHM, this->getPropertyValueAsString(
                     wrench::MultihostMulticoreComputeServiceProperty::TASK_SCHEDULING_CORE_ALLOCATION_ALGORITHM)},
             {wrench::StandardJobExecutorProperty::TASK_SELECTION_ALGORITHM,  this->getPropertyValueAsString(
                     wrench::MultihostMulticoreComputeServiceProperty::TASK_SCHEDULING_TASK_SELECTION_ALGORITHM)},
             {wrench::StandardJobExecutorProperty::HOST_SELECTION_ALGORITHM,  this->getPropertyValueAsString(
                     wrench::MultihostMulticoreComputeServiceProperty::TASK_SCHEDULING_HOST_SELECTION_ALGORITHM)}});

    this->standard_job_executors.insert(std::unique_ptr<wrench::StandardJobExecutor>(executor));
    this->running_jobs.insert(job);

    // Tell the caller that a job was dispatched!
    return true;
}












