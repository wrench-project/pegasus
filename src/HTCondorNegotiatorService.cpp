/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "HTCondorCentralManagerServiceMessage.h"
#include "HTCondorNegotiatorService.h"
#include "HTCondorCentralManagerServiceMessagePayload.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(htcondor_negotiator, "Log category for HTCondorNegotiator");

namespace wrench {
    namespace pegasus {

        /**
         * @brief Constructor
         *
         * @param hostname: the hostname on which to start the service
         * @param compute_resources: a set of compute resources available via the HTCondor pool
         * @param pending_jobs: a list of pending jobs
         * @param reply_mailbox: the mailbox to which the "done/failed" message should be sent
         */
        HTCondorNegotiatorService::HTCondorNegotiatorService(std::string &hostname,
                                                             std::set<std::shared_ptr<ComputeService>> &compute_resources,
                                                             std::vector<StandardJob *> &pending_jobs,
                                                             std::string &reply_mailbox)
                : Service(hostname, "htcondor_negotiator", "htcondor_negotiator"), reply_mailbox(reply_mailbox),
                  compute_resources(compute_resources), pending_jobs(pending_jobs) {

          this->setMessagePayloads(this->default_messagepayload_values, messagepayload_list);
        }

        /**
         * @brief Destructor
         */
        HTCondorNegotiatorService::~HTCondorNegotiatorService() {
          this->pending_jobs.clear();
          this->compute_resources.clear();
        }

        /**
         * @brief Main method of the daemon
         *
         * @return 0 on termination
         */
        int HTCondorNegotiatorService::main() {

          TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_BLUE);
          WRENCH_INFO("HTCondor Negotiator Service starting on host %s listening on mailbox_name %s",
                      this->hostname.c_str(), this->mailbox_name.c_str());

          // TODO: check how to pass the service specific arguments
          std::map<std::string, std::string> specific_args;

          std::vector<StandardJob *> scheduled_jobs;

          for (auto job : this->pending_jobs) {
            for (auto &&cs : this->compute_resources) {
              unsigned long sum_num_idle_cores;
              std::vector<unsigned long> num_idle_cores = cs->getNumIdleCores();
              sum_num_idle_cores = std::accumulate(num_idle_cores.begin(), num_idle_cores.end(), 0ul);

              if (sum_num_idle_cores >= job->getMinimumRequiredNumCores()) {
                WRENCH_INFO("Dispatching job %s with %ld tasks", job->getName().c_str(), job->getTasks().size());
                job->pushCallbackMailbox(this->reply_mailbox);
                cs->submitStandardJob(job, specific_args);
                scheduled_jobs.push_back(job);
                WRENCH_INFO("Dispatched job %s with %ld tasks", job->getName().c_str(), job->getTasks().size());
                break;
              }
            }
          }

          // Send the callback to the originator
          try {
            S4U_Mailbox::putMessage(
                    this->reply_mailbox, new NegotiatorCompletionMessage(
                            scheduled_jobs, this->getMessagePayloadValueAsDouble(
                                    HTCondorCentralManagerServiceMessagePayload::HTCONDOR_NEGOTIATOR_DONE_MESSAGE_PAYLOAD)));
          } catch (std::shared_ptr<NetworkError> &cause) {
            return 1;
          }

          WRENCH_INFO("HTCondorNegotiator Service on host %s terminated!", S4U_Simulation::getHostName().c_str());
          return 0;
        }

    }
}
