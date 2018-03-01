/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "HTCondorService.h"
#include "wrench-dev.h"


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
         * @param execution_hosts: a list of execution hosts available to create the HTCondor pool
         * @param default_storage_service: a storage service (or nullptr)
         * @param plist: a property list ({} means "use all defaults")
         *
         * @throw std::runtime_error
         */
        HTCondorService::HTCondorService(const std::string &hostname,
                                         const std::string &pool_name,
                                         bool supports_standard_jobs,
                                         bool supports_pilot_jobs,
                                         std::vector<std::string> &execution_hosts,
                                         StorageService *default_storage_service,
                                         std::map<std::string, std::string> plist) :
                ComputeService(hostname, "htcondor_service", "htcondor_service", supports_standard_jobs,
                               supports_pilot_jobs, default_storage_service) {

          std::set<std::unique_ptr<ComputeService>> compute_resources;

          for (std::string execution_host : execution_hosts) {
            this->simulation->getHostNumCores(execution_host);
            compute_resources.insert(std::unique_ptr<ComputeService>(
                    new MultihostMulticoreComputeService(execution_host, supports_standard_jobs, supports_pilot_jobs,
                                                         {std::make_tuple(execution_host,
                                                                          this->simulation->getHostNumCores(
                                                                                  execution_host),
                                                                          this->simulation->getHostMemoryCapacity(
                                                                                  execution_host))},
                                                         default_storage_service, plist)));
          }

          initiateInstance(hostname, pool_name, compute_resources, plist);
        }

        /**
         * @brief Constructor
         *
         * @param hostname: the hostname on which to start the service
         * @param pool_name: HTCondor pool name
         * @param supports_standard_jobs: true if the HTCondor service should support standard jobs
         * @param supports_pilot_jobs: true if the HTCondor service should support pilot jobs
         * @param compute_resources: a set of compute resources available via the HTCondor pool
         * @param default_storage_service: a storage service (or nullptr)
         * @param plist: a property list ({} means "use all defaults")
         *
         * @throw std::runtime_error
         */
        HTCondorService::HTCondorService(const std::string &hostname,
                                         const std::string &pool_name,
                                         bool supports_standard_jobs,
                                         bool supports_pilot_jobs,
                                         std::set<std::unique_ptr<ComputeService>> &compute_resources,
                                         StorageService *default_storage_service,
                                         std::map<std::string, std::string> plist) :
                ComputeService(hostname, "htcondor_service", "htcondor_service", supports_standard_jobs,
                               supports_pilot_jobs, default_storage_service) {

          initiateInstance(hostname, pool_name, compute_resources, plist);
        }

        /**
         * @brief Destructor
         */
        HTCondorService::~HTCondorService() {
          this->default_property_values.clear();
        }

        /**
         * @brief Instantiate the HTCondor service
         *
         * @param hostname: the hostname on which to start the service
         * @param pool_name: HTCondor pool name
         * @param supports_standard_jobs: true if the HTCondor service should support standard jobs
         * @param supports_pilot_jobs: true if the HTCondor service should support pilot jobs
         * @param compute_resources: a set of compute resources available via the HTCondor pool
         * @param default_storage_service: a storage service (or nullptr)
         * @param plist: a property list ({} means "use all defaults")
         *
         * @throw std::runtime_error
         */
        void HTCondorService::initiateInstance(const std::string &hostname,
                                               const std::string &pool_name,
                                               std::set<std::unique_ptr<ComputeService>> &compute_resources,
                                               std::map<std::string, std::string> plist) {

          if (pool_name.empty()) {
            throw std::runtime_error("A pool name for the HTCondor service should be provided.");
          }
          if (compute_resources.empty()) {
            throw std::runtime_error("At least one compute service should be provided");
          }
          this->pool_name = pool_name;
          this->compute_resources = std::move(compute_resources);

          // Set default and specified properties
          this->setProperties(this->default_property_values, plist);
        }

        /**
         * @brief Start the service
         *
         * @throw std::runtime_error
         */
        void HTCondorService::start() {
          Service::start();

          for (auto &&cs : this->compute_resources) {
            cs->start();
          }
        }

        /**
         * @brief Submit a standard job to the cloud service
         *
         * @param job: a standard job
         * @param service_specific_args: service specific arguments
         *
         * @throw WorkflowExecutionException
         * @throw std::runtime_error
         */
        void HTCondorService::submitStandardJob(StandardJob *job,
                                                std::map<std::string, std::string> &service_specific_args) {
          throw std::runtime_error("CloudService::terminateStandardJob(): Not implemented yet!");
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

          TerminalOutput::setThisProcessLoggingColor(WRENCH_LOGGING_COLOR_RED);
          WRENCH_INFO("HTCondor Service starting on host %s listening on mailbox_name %s", this->hostname.c_str(),
                      this->mailbox_name.c_str());

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

          } else {
            throw std::runtime_error("Unexpected [" + message->getName() + "] message");
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
