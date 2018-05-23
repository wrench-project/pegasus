/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <numeric>
#include <wrench-dev.h>

#include "HTCondorService.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(HTCondor, "Log category for HTCondorService Scheduler");

namespace wrench {
    namespace pegasus {

        /**
         * @brief Constructor
         *
         * @param hostname: the hostname on which to start the service
         * @param pool_name: HTCondor pool name
         * @param supports_standard_jobs: true if the HTCondor service should support standard jobs
         * @param supports_pilot_jobs: true if the HTCondor service should support pilot jobs
         * @param compute_resources: a set of compute resources available via the HTCondor pool
         * @param plist: a property list ({} means "use all defaults")
         *
         * @throw std::runtime_error
         */
        HTCondorService::HTCondorService(const std::string &hostname,
                                         const std::string &pool_name,
                                         bool supports_standard_jobs,
                                         bool supports_pilot_jobs,
                                         std::set<std::shared_ptr<ComputeService>> compute_resources,
                                         std::map<std::string, std::string> plist) :
                ComputeService(hostname, "htcondor_service", "htcondor_service", supports_standard_jobs,
                               supports_pilot_jobs, nullptr) {

          if (pool_name.empty()) {
            throw std::runtime_error("A pool name for the HTCondor service should be provided.");
          }
          if (compute_resources.empty()) {
            throw std::runtime_error("At least one compute service should be provided");
          }
          this->pool_name = pool_name;
          this->compute_resources = compute_resources;

          // setting simulation object for compute resources
          for (auto &&cs : this->compute_resources) {
            cs->simulation = this->simulation;
          }

          // Set default and specified properties
          this->setProperties(this->default_property_values, plist);
        }

        /**
         * @brief Destructor
         */
        HTCondorService::~HTCondorService() {
          this->default_property_values.clear();
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
        void HTCondorService::submitStandardJob(StandardJob *job,
                                                std::map<std::string, std::string> &service_specific_args) {

          serviceSanityCheck();

          std::string answer_mailbox = S4U_Mailbox::generateUniqueMailboxName("submit_standard_job");

          //  send a "run a standard job" message to the daemon's mailbox_name
          try {
            S4U_Mailbox::putMessage(this->mailbox_name,
                                    new ComputeServiceSubmitStandardJobRequestMessage(
                                            answer_mailbox, job, service_specific_args,
                                            this->getPropertyValueAsDouble(
                                                    ComputeServiceProperty::SUBMIT_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD)));
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
        void HTCondorService::submitPilotJob(PilotJob *job, std::map<std::string, std::string> &service_specific_args) {
          throw std::runtime_error("HTCondorService::terminateStandardJob(): Not implemented yet!");
        }


        /**
         * @brief Terminate a standard job to the compute service (virtual)
         * @param job: the job
         *
         * @throw std::runtime_error
         */
        void HTCondorService::terminateStandardJob(StandardJob *job) {
          throw std::runtime_error("HTCondorService::terminateStandardJob(): Not implemented yet!");
        }

        /**
         * @brief Terminate a pilot job to the compute service
         * @param job: the job
         *
         * @throw std::runtime_error
         */
        void HTCondorService::terminatePilotJob(PilotJob *job) {
          throw std::runtime_error("HTCondorService::terminatePilotJob(): Not implemented yet!");
        }

        /**
         * @brief Main method of the daemon
         *
         * @return 0 on termination
        */
        int HTCondorService::main() {

          TerminalOutput::setThisProcessLoggingColor(COLOR_RED);
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

          /** Main loop **/
          while (this->processNextMessage()) {
            // no specific action
          }

          WRENCH_INFO("HTCondor Service on host %s terminated!", S4U_Simulation::getHostName().c_str());
          return 0;
        }

        /**
         * @brief Wait for and react to any incoming message
         *
         * @return false if the daemon should terminate, true otherwise
         *
         * @throw std::runtime_error
         */
        bool HTCondorService::processNextMessage() {
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
              S4U_Mailbox::putMessage(msg->ack_mailbox,
                                      new ServiceDaemonStoppedMessage(this->getPropertyValueAsDouble(
                                              HTCondorServiceProperty::DAEMON_STOPPED_MESSAGE_PAYLOAD)));
            } catch (std::shared_ptr<NetworkError> &cause) {
              return false;
            }
            return false;

          } else if (auto *msg = dynamic_cast<ComputeServiceResourceInformationRequestMessage *>(message.get())) {
            processGetResourceInformation(msg->answer_mailbox);
            return true;

          } else if (auto *msg = dynamic_cast<ComputeServiceSubmitStandardJobRequestMessage *>(message.get())) {
            processSubmitStandardJob(msg->answer_mailbox, msg->job, msg->service_specific_args);
            return true;

          } else {
            throw std::runtime_error("Unexpected [" + message->getName() + "] message");
          }
        }

        /**
         * @brief Process a "get resource information message"
         *
         * @param answer_mailbox: the mailbox to which the description message should be sent
         */
        void HTCondorService::processGetResourceInformation(const std::string &answer_mailbox) {

          // Build a dictionary
          std::map<std::string, std::vector<double>> dict;

          // Num hosts
          std::vector<double> num_hosts;
          num_hosts.push_back((double) this->compute_resources.size());
          dict.insert(std::make_pair("num_hosts", num_hosts));

          std::vector<double> num_cores;
          std::vector<double> num_idle_cores;
          std::vector<double> flop_rates;
          std::vector<double> ram_capacities;
          std::vector<double> ram_availabilities;

          for (auto &&cs : this->compute_resources) {
            // Num cores per compute resource
            std::vector<unsigned long> ncores = cs->getNumCores();
            num_cores.push_back(std::accumulate(ncores.begin(), ncores.end(), 0));

            // Num idle cores per compute resource
            std::vector<unsigned long> nicores = cs->getNumIdleCores();
            num_idle_cores.push_back(std::accumulate(nicores.begin(), nicores.end(), 0));

            // Flop rate per compute resource
            flop_rates.push_back(S4U_Simulation::getFlopRate(cs->getHostname()));

            // RAM capacity per host
            ram_capacities.push_back(S4U_Simulation::getHostMemoryCapacity(cs->getHostname()));

            // RAM availability per host
            ram_availabilities.push_back(
                    ComputeService::ALL_RAM);  // TODO FOR RAFAEL : What about VM memory availabilities???
          }

          dict.insert(std::make_pair("num_cores", num_cores));
          dict.insert(std::make_pair("num_idle_cores", num_idle_cores));
          dict.insert(std::make_pair("flop_rates", flop_rates));
          dict.insert(std::make_pair("ram_capacities", ram_capacities));
          dict.insert(std::make_pair("ram_availabilities", ram_availabilities));

          std::vector<double> ttl;
          ttl.push_back(ComputeService::ALL_RAM);
          dict.insert(std::make_pair("ttl", ttl));

          // Send the reply
          ComputeServiceResourceInformationAnswerMessage *answer_message = new ComputeServiceResourceInformationAnswerMessage(
                  dict,
                  this->getPropertyValueAsDouble(
                          ComputeServiceProperty::RESOURCE_DESCRIPTION_ANSWER_MESSAGE_PAYLOAD));
          try {
            S4U_Mailbox::dputMessage(answer_mailbox, answer_message);
          } catch (std::shared_ptr<NetworkError> &cause) {
            return;
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
        void HTCondorService::processSubmitStandardJob(const std::string &answer_mailbox, StandardJob *job,
                                                       std::map<std::string, std::string> &service_specific_args) {

          WRENCH_INFO("Asked to run a standard job with %ld tasks", job->getNumTasks());
          if (not this->supportsStandardJobs()) {
            try {
              S4U_Mailbox::dputMessage(
                      answer_mailbox,
                      new ComputeServiceSubmitStandardJobAnswerMessage(
                              job, this, false, std::shared_ptr<FailureCause>(new JobTypeNotSupported(job, this)),
                              this->getPropertyValueAsDouble(
                                      ComputeServiceProperty::SUBMIT_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD)));
            } catch (std::shared_ptr<NetworkError> &cause) {
              return;
            }
            return;
          }

          for (auto &&cs : this->compute_resources) {
            unsigned long sum_num_idle_cores;
            std::vector<unsigned long> num_idle_cores = cs->getNumIdleCores();
            sum_num_idle_cores = std::accumulate(num_idle_cores.begin(), num_idle_cores.end(), 0ul);

            if (sum_num_idle_cores >= job->getMinimumRequiredNumCores()) {
              cs->submitStandardJob(job, service_specific_args);
              try {
                S4U_Mailbox::dputMessage(
                        answer_mailbox,
                        new ComputeServiceSubmitStandardJobAnswerMessage(
                                job, this, true, nullptr, this->getPropertyValueAsDouble(
                                        ComputeServiceProperty::SUBMIT_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD)));
                return;
              } catch (std::shared_ptr<NetworkError> &cause) {
                return;
              }
            }
          }

          // could not find a suitable resource
          try {
            S4U_Mailbox::dputMessage(
                    answer_mailbox,
                    new ComputeServiceSubmitStandardJobAnswerMessage(
                            job, this, false, std::shared_ptr<FailureCause>(new NotEnoughComputeResources(job, this)),
                            this->getPropertyValueAsDouble(
                                    ComputeServiceProperty::SUBMIT_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD)));
          } catch (std::shared_ptr<NetworkError> &cause) {
            return;
          }
        }

        /**
         * @brief Terminate the daemon.
         */
        void HTCondorService::terminate() {
          this->setStateToDown();

          WRENCH_INFO("Stopping Compute Services");
          for (auto &&cs : this->compute_resources) {
            cs->stop();
          }
        }
    }
}
