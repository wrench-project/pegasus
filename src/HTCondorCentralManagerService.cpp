/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <wrench-dev.h>
#include "HTCondorCentralManagerService.h"
#include "HTCondorCentralManagerServiceMessage.h"
#include "HTCondorNegotiatorService.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(HTCondorCentralManager, "Log category for HTCondorCentralManagerService");

namespace wrench {
    namespace pegasus {

        /**
         * @brief Constructor
         *
         * @param hostname: the hostname on which to start the service
         * @param compute_resources: a set of compute resources available via the HTCondor pool
         * @param property_list: a property list ({} means "use all defaults")
         * @param messagepayload_list: a message payload list ({} means "use all defaults")
         *
         * @throw std::runtime_error
         */
        HTCondorCentralManagerService::HTCondorCentralManagerService(
                const std::string &hostname,
                std::set<std::shared_ptr<ComputeService>> compute_resources,
                std::map<std::string, std::string> property_list,
                std::map<std::string, std::string> messagepayload_list)
                : ComputeService(hostname, "htcondor_central_manager", "htcondor_central_manager", nullptr) {

          if (compute_resources.empty()) {
            throw std::runtime_error("At least one compute service should be provided");
          }
          this->compute_resources = compute_resources;

          // Set default and specified properties
          this->setProperties(this->default_property_values, std::move(property_list));

          // Set default and specified message payloads
          this->setMessagePayloads(this->default_messagepayload_values, std::move(messagepayload_list));
        }

        /**
         * @brief Destructor
         */
        HTCondorCentralManagerService::~HTCondorCentralManagerService() {
          this->pending_jobs.clear();
          this->compute_resources.clear();
        }

        /**
         * @brief Submit a standard job to the HTCondor service
         *
         * @param job: a standard job
         * @param service_specific_args: service specific arguments
         *
         * @throw WorkflowExecutionException
         * @throw std::runtime_error
         */
        void HTCondorCentralManagerService::submitStandardJob(
                StandardJob *job,
                std::map<std::string, std::string> &service_specific_args) {

          serviceSanityCheck();

          std::string answer_mailbox = S4U_Mailbox::generateUniqueMailboxName("submit_standard_job");

          //  send a "run a standard job" message to the daemon's mailbox_name
          try {
            S4U_Mailbox::putMessage(
                    this->mailbox_name,
                    new ComputeServiceSubmitStandardJobRequestMessage(
                            answer_mailbox, job, service_specific_args,
                            this->getMessagePayloadValueAsDouble(
                                    HTCondorCentralManagerServiceMessagePayload::SUBMIT_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD)));
          } catch (std::shared_ptr<NetworkError> &cause) {
            throw WorkflowExecutionException(cause);
          }

          // Get the answer
          std::unique_ptr<SimulationMessage> message = nullptr;
          try {
            message = S4U_Mailbox::getMessage(answer_mailbox);
          } catch (std::shared_ptr<NetworkError> &cause) {
            throw WorkflowExecutionException(cause);
          }

          if (auto *msg = dynamic_cast<ComputeServiceSubmitStandardJobAnswerMessage *>(message.get())) {
            // If no success, throw an exception
            if (not msg->success) {
              throw WorkflowExecutionException(msg->failure_cause);
            }
          } else {
            throw std::runtime_error(
                    "ComputeService::submitStandardJob(): Received an unexpected [" + message->getName() +
                    "] message!");
          }
        }

        /**
         * @brief Asynchronously submit a pilot job to the cloud service
         *
         * @param job: a pilot job
         * @param service_specific_args: service specific arguments
         *
         * @throw WorkflowExecutionException
         * @throw std::runtime_error
         */
        void HTCondorCentralManagerService::submitPilotJob(PilotJob *job,
                                                           std::map<std::string, std::string> &service_specific_args) {
          throw std::runtime_error("HTCondorCentralManagerService::terminateStandardJob(): Not implemented yet!");
        }


        /**
         * @brief Terminate a standard job to the compute service (virtual)
         * @param job: the job
         *
         * @throw std::runtime_error
         */
        void HTCondorCentralManagerService::terminateStandardJob(StandardJob *job) {
          throw std::runtime_error("HTCondorCentralManagerService::terminateStandardJob(): Not implemented yet!");
        }

        /**
         * @brief Terminate a pilot job to the compute service
         * @param job: the job
         *
         * @throw std::runtime_error
         */
        void HTCondorCentralManagerService::terminatePilotJob(PilotJob *job) {
          throw std::runtime_error("HTCondorCentralManagerService::terminatePilotJob(): Not implemented yet!");
        }

        /**
         * @brief Main method of the daemon
         *
         * @return 0 on termination
         */
        int HTCondorCentralManagerService::main() {

          TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_BLUE);
          WRENCH_INFO("HTCondor Service starting on host %s listening on mailbox_name %s", this->hostname.c_str(),
                      this->mailbox_name.c_str());

          // start the compute resource services
          try {
            for (auto &cs : this->compute_resources) {
              cs->simulation = this->simulation;
              cs->start(cs, true); // Daemonize!
            }
          } catch (std::runtime_error &e) {
            throw;
          }

          // main loop
          while (this->processNextMessage()) {
            // starting an HTCondor negotiator
            if (not this->dispatching_jobs && not this->pending_jobs.empty()) {
              this->dispatching_jobs = true;
              auto negotiator = std::make_shared<HTCondorNegotiatorService>(this->hostname, this->compute_resources,
                                                                            this->pending_jobs, this->mailbox_name);
              negotiator->simulation = this->simulation;
              negotiator->start(negotiator, true);
            }
          }

          WRENCH_INFO("HTCondorCentralManager Service on host %s terminated!", S4U_Simulation::getHostName().c_str());
          return 0;
        }

//        /**
//         * @brief Dispatch one pending job, if possible
//         */
//        void HTCondorCentralManagerService::dispatchPendingJobs() {
//
//          // TODO: check how to pass the service specific arguments
//          std::map<std::string, std::string> specific_args;
//
//          bool submitted = false;
//
//          for (auto it = this->scheduled_jobs.begin(); it != this->scheduled_jobs.end();) {
//            auto job = *it;
//
//            for (auto &&cs : this->compute_resources) {
//              unsigned long sum_num_idle_cores;
//              std::vector<unsigned long> num_idle_cores = cs->getNumIdleCores();
//              sum_num_idle_cores = std::accumulate(num_idle_cores.begin(), num_idle_cores.end(), 0ul);
//
//              if (sum_num_idle_cores >= job->getMinimumRequiredNumCores()) {
//                WRENCH_INFO("Dispatching job %s with %ld tasks", job->getName().c_str(), job->getTasks().size());
//                job->pushCallbackMailbox(this->mailbox_name);
//                cs->submitStandardJob(job, specific_args);
//                this->scheduled_jobs.erase(it);
//                submitted = true;
//                WRENCH_INFO("Dispatched job %s with %ld tasks", job->getName().c_str(), job->getTasks().size());
//                break;
//              }
//            }
//
//            if (not submitted) {
//              ++it;
//            }
//          }
//        }

        /**
         * @brief Wait for and react to any incoming message
         *
         * @return false if the daemon should terminate, true otherwise
         *
         * @throw std::runtime_error
         */
        bool HTCondorCentralManagerService::processNextMessage() {
          // Wait for a message
          std::unique_ptr<SimulationMessage> message;

          try {
            message = S4U_Mailbox::getMessage(this->mailbox_name);
          } catch (std::shared_ptr<NetworkError> &cause) {
            return true;
          }

          if (message == nullptr) {
            WRENCH_INFO("Got a NULL message... Likely this means we're all done. Aborting");
            return false;
          }

          WRENCH_INFO("Got a [%s] message", message->getName().c_str());

          if (auto *msg = dynamic_cast<ServiceStopDaemonMessage *>(message.get())) {
            this->terminate();
            // This is Synchronous
            try {
              S4U_Mailbox::putMessage(
                      msg->ack_mailbox,
                      new ServiceDaemonStoppedMessage(
                              this->getMessagePayloadValueAsDouble(
                                      HTCondorCentralManagerServiceMessagePayload::DAEMON_STOPPED_MESSAGE_PAYLOAD)));
            } catch (std::shared_ptr<NetworkError> &cause) {
              return false;
            }
            return false;

          } else if (auto *msg = dynamic_cast<ComputeServiceSubmitStandardJobRequestMessage *>(message.get())) {
            processSubmitStandardJob(msg->answer_mailbox, msg->job, msg->service_specific_args);
            return true;

          } else if (auto *msg = dynamic_cast<ComputeServiceStandardJobDoneMessage *>(message.get())) {
            processStandardJobCompletion(msg->job);
            return true;

          } else if (auto *msg = dynamic_cast<NegotiatorCompletionMessage *>(message.get())) {
            processNegotiatorCompletion(msg->scheduled_jobs);
            return true;

          } else {
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
        void HTCondorCentralManagerService::processSubmitStandardJob(
                const std::string &answer_mailbox, StandardJob *job,
                std::map<std::string, std::string> &service_specific_args) {

          this->pending_jobs.push_back(job);

          try {
            S4U_Mailbox::dputMessage(
                    answer_mailbox,
                    new ComputeServiceSubmitStandardJobAnswerMessage(
                            job, this, true, nullptr, this->getMessagePayloadValueAsDouble(
                                    HTCondorCentralManagerServiceMessagePayload::SUBMIT_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD)));
          } catch (std::shared_ptr<NetworkError> &cause) {
          }
        }

        /**
         * @brief Process a standard job completion
         *
         * @param job: the job
         *
         * @throw std::runtime_error
         */
        void HTCondorCentralManagerService::processStandardJobCompletion(StandardJob *job) {
          WRENCH_INFO("A standard job has completed job %s", job->getName().c_str());

          // Send the callback to the originator
          try {
            S4U_Mailbox::dputMessage(
                    job->popCallbackMailbox(), new ComputeServiceStandardJobDoneMessage(
                            job, this, this->getMessagePayloadValueAsDouble(
                                    HTCondorCentralManagerServiceMessagePayload::STANDARD_JOB_DONE_MESSAGE_PAYLOAD)));
          } catch (std::shared_ptr<NetworkError> &cause) {
          }
        }

        /**
         * @brief Process a negotiator cycle completion
         *
         * @param pending_jobs: list of pending jobs upon negotiator cycle completion
         */
        void HTCondorCentralManagerService::processNegotiatorCompletion(
                std::vector<wrench::StandardJob *> pending_jobs) {

          WRENCH_INFO("--------------------------- NEW PENDING JOBS: %ld", pending_jobs.size());
          for (auto sjob : pending_jobs) {
            for (auto it = this->pending_jobs.begin(); it != this->pending_jobs.end(); ++it) {
              auto pjob = *it;
              if (sjob == pjob) {
                this->pending_jobs.erase(it);
                break;
              }
            }
          }
//          this->scheduled_jobs = scheduled_jobs;
          this->dispatching_jobs = false;
        }

        /**
         * @brief Terminate the daemon.
         */
        void HTCondorCentralManagerService::terminate() {
          this->setStateToDown();
          WRENCH_INFO("Stopping Compute Services");
          for (auto &cs : this->compute_resources) {
            cs->stop();
          }
        }

    }
}
