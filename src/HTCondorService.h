/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef WRENCH_PEGASUS_HTCONDOR_H
#define WRENCH_PEGASUS_HTCONDOR_H

#include <wrench-dev.h>
#include "HTCondorServiceProperty.h"

namespace wrench {
    namespace pegasus {

        class Simulation;

        /**
         * @brief A multicore Scheduler for schedd
         *
         */
        class HTCondorService : public ComputeService {
        private:
            std::map<std::string, std::string> default_property_values = {
                    {HTCondorServiceProperty::STOP_DAEMON_MESSAGE_PAYLOAD,                  "1024"},
                    {HTCondorServiceProperty::DAEMON_STOPPED_MESSAGE_PAYLOAD,               "1024"},
                    {HTCondorServiceProperty::RESOURCE_DESCRIPTION_REQUEST_MESSAGE_PAYLOAD, "1024"},
                    {HTCondorServiceProperty::RESOURCE_DESCRIPTION_ANSWER_MESSAGE_PAYLOAD,  "1024"},
                    {HTCondorServiceProperty::SUBMIT_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD,  "1024"},
                    {HTCondorServiceProperty::SUBMIT_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD,   "1024"},
                    {HTCondorServiceProperty::SUBMIT_PILOT_JOB_REQUEST_MESSAGE_PAYLOAD,     "1024"},
                    {HTCondorServiceProperty::SUBMIT_PILOT_JOB_ANSWER_MESSAGE_PAYLOAD,      "1024"}
            };

        public:
            HTCondorService(const std::string &hostname,
                            const std::string &pool_name,
                            bool supports_standard_jobs,
                            bool supports_pilot_jobs,
                            std::vector<std::string> &execution_hosts,
                            StorageService *default_storage_service,
                            std::map<std::string, std::string> plist = {});

            HTCondorService(const std::string &hostname,
                            const std::string &pool_name,
                            bool supports_standard_jobs,
                            bool supports_pilot_jobs,
                            std::set<std::shared_ptr<ComputeService>> &compute_resources,
                            StorageService *default_storage_service,
                            std::map<std::string, std::string> plist = {});

            /***********************/
            /** \cond DEVELOPER   **/
            /***********************/

            void submitStandardJob(StandardJob *job,
                                   std::map<std::string, std::string> &service_specific_arguments) override;

            void submitPilotJob(PilotJob *job, std::map<std::string, std::string> &service_specific_arguments) override;

            /***********************/
            /** \cond INTERNAL    */
            /***********************/

            ~HTCondorService();

            void terminateStandardJob(StandardJob *job) override;

            void terminatePilotJob(PilotJob *job) override;

            /***********************/
            /** \endcond          **/
            /***********************/

        private:

            int main() override;

            void initiateInstance(const std::string &hostname,
                                  const std::string &pool_name,
                                  std::set<std::shared_ptr<ComputeService>> &compute_resources,
                                  std::map<std::string, std::string> plist);

            bool processNextMessage();

            void processGetResourceInformation(const std::string &answer_mailbox);

            void processSubmitStandardJob(const std::string &answer_mailbox, StandardJob *job,
                                          std::map<std::string, std::string> &service_specific_args);

            void terminate();

            std::string pool_name;
            std::set<std::shared_ptr<ComputeService>> compute_resources;
        };
    }
}

#endif //WRENCH_PEGASUS_HTCONDOR_H
