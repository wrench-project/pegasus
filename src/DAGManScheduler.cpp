/**
 * Copyright (c) 2017-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "DAGManScheduler.h"
#include "PegasusSimulationTimestampTypes.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(HTCondorSchedd, "Log category for HTCondor Scheduler Daemon");

namespace wrench {
    namespace pegasus {

        /**
         * @brief Constructor
         *
         * @param file_registry_service: a pointer to the file registry service
         * @param storage_services: a set of storage services available for the scheduler
         */
        DAGManScheduler::DAGManScheduler(std::shared_ptr<FileRegistryService> &file_registry_service,
                                         const std::set<std::shared_ptr<StorageService>> &storage_services)
                : file_registry_service(file_registry_service), storage_services(storage_services) {}

        /**
         * @brief Destructor
         */
        DAGManScheduler::~DAGManScheduler() {}

        /**
         * @brief Schedule and run a set of ready tasks on available HTCondor resources
         *
         * @param compute_services: a set of compute services available to run jobs
         * @param tasks: a map of (ready) workflow tasks
         *
         * @throw std::runtime_error
         */
        void DAGManScheduler::scheduleTasks(const std::set<std::shared_ptr<ComputeService>> &compute_services,
                                            const std::vector<WorkflowTask *> &tasks) {

            WRENCH_INFO("There are %ld ready tasks to schedule", tasks.size());

            // TODO: select htcondor service based on condor queue name
            auto htcondor_service = std::dynamic_pointer_cast<HTCondorComputeService>(*compute_services.begin());

            unsigned long scheduled_tasks = 0;

            for (auto task : tasks) {
                WorkflowJob *job = nullptr;

                // check whether files need to be staged in
                for (auto file : task->getInputFiles()) {
                    if (not htcondor_service->getLocalStorageService()->lookupFile(
                            file, FileLocation::LOCATION(htcondor_service->getLocalStorageService(), "/"))) {

                        auto file_locations = this->file_registry_service->lookupEntry(file);
                        this->getDataMovementManager()->doSynchronousFileCopy(
                                file, *file_locations.begin(),
                                FileLocation::LOCATION(htcondor_service->getLocalStorageService(), "/"));
                    }
                }

                // create job start event
                this->simulation->getOutput().addTimestamp<SimulationTimestampJobSubmitted>(
                        new SimulationTimestampJobSubmitted(task));

                // finding the file locations
                std::map<WorkflowFile *, std::shared_ptr<FileLocation>> file_locations;
                for (auto f : task->getInputFiles()) {
                    file_locations[f] = FileLocation::LOCATION(htcondor_service->getLocalStorageService());
                }
                for (auto f : task->getOutputFiles()) {
                    file_locations[f] = FileLocation::LOCATION(htcondor_service->getLocalStorageService());
                }

                // creating job for execution
                job = (WorkflowJob *) this->getJobManager()->createStandardJob(task, file_locations);

                WRENCH_INFO("Scheduling task: %s", task->getID().c_str());
                this->getJobManager()->submitJob(job, htcondor_service);
                // create job scheduled event
                this->simulation->getOutput().addTimestamp<SimulationTimestampJobScheduled>(
                        new SimulationTimestampJobScheduled(task));WRENCH_INFO("Scheduled task: %s",
                                                                               task->getID().c_str());
                scheduled_tasks++;
            }

            WRENCH_INFO("Done with scheduling tasks as standard jobs: %ld tasks scheduled out of %ld", scheduled_tasks,
                        tasks.size());
        }

        /**
         * @brief
         *
         * @param simulation: a pointer to the simulation object
         */
        void DAGManScheduler::setSimulation(wrench::Simulation *simulation) {
            this->simulation = simulation;
        }

        /**
         * @brief:
         *
         * @param monitor_callback_mailbox
         */
        void DAGManScheduler::setMonitorCallbackMailbox(std::string monitor_callback_mailbox) {
            this->monitor_callback_mailbox = monitor_callback_mailbox;
        }

    }
}
