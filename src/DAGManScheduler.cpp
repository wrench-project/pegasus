/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "DAGManScheduler.h"
#include "wrench/services/compute/htcondor/HTCondorService.h"
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
        DAGManScheduler::DAGManScheduler(FileRegistryService *file_registry_service,
                                       const std::set<StorageService *> &storage_services)
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
        void DAGManScheduler::scheduleTasks(const std::set<ComputeService *> &compute_services,
                                           const std::vector<WorkflowTask *> &tasks) {

          WRENCH_INFO("There are %ld ready tasks to schedule", tasks.size());

          // TODO: select htcondor service based on condor queue name
          auto htcondor_service = (HTCondorService *) *(compute_services.begin());

          unsigned long scheduled_tasks = 0;

          for (auto task : tasks) {
            WorkflowJob *job = nullptr;

            if (task->getTaskType() == WorkflowTask::TaskType::TRANSFER) {
              // data stage in/out task

              std::map<WorkflowFile *, StorageService *> file_locations;
              std::set<std::tuple<WorkflowFile *, StorageService *, StorageService *>> pre_file_copies;
              std::set<std::tuple<WorkflowFile *, StorageService *, StorageService *>> post_file_copies;

              // input files
              for (auto input_file : task->getInputFiles()) {
                // finding storage service
                auto src_dest = (*task->getFileTransfers().find(input_file)).second;

                StorageService *src =
                        src_dest.first == "local" ? htcondor_service->getLocalStorageService() : nullptr;
                StorageService *dest =
                        src_dest.second == "local" ? htcondor_service->getLocalStorageService() : nullptr;

                for (auto storage_service : this->storage_services) {
                  if (!src && storage_service->getHostname() == src_dest.first) {
                    src = storage_service;
                  } else if (!dest && storage_service->getHostname() == src_dest.second) {
                    dest = storage_service;
                  }
                }

                if (src != dest) {
                  pre_file_copies.insert(std::make_tuple(input_file, src, dest));
                }
                file_locations.insert(std::make_pair(input_file, dest));
              }

              // output files
              for (auto output_file : task->getOutputFiles()) {
                // finding storage service
                auto src_dest = (*task->getFileTransfers().find(output_file)).second;

                StorageService *src =
                        src_dest.first == "local" ? htcondor_service->getLocalStorageService() : nullptr;
                StorageService *dest =
                        src_dest.second == "local" ? htcondor_service->getLocalStorageService() : nullptr;

                for (auto storage_service : this->storage_services) {
                  if (!src && storage_service->getHostname() == src_dest.first) {
                    src = storage_service;
                  } else if (!dest && storage_service->getHostname() == src_dest.second) {
                    dest = storage_service;
                  }
                }

                if (src != dest) {
                  post_file_copies.insert(std::make_tuple(output_file, src, dest));
                }
                file_locations.insert(std::make_pair(output_file, dest));
              }

              // creating and submitting job
              job = (WorkflowJob *) this->getJobManager()->createStandardJob({task},
                                                                             file_locations,
                                                                             pre_file_copies,
                                                                             post_file_copies, {});

            } else { // regular compute task

              // input/output files
              std::map<WorkflowFile *, StorageService *> file_locations;
              for (auto f : task->getInputFiles()) {
                file_locations.insert(std::make_pair(f, htcondor_service->getLocalStorageService()));
              }
              for (auto f : task->getOutputFiles()) {
                file_locations.insert(std::make_pair(f, htcondor_service->getLocalStorageService()));
              }

              // create job start event
              this->simulation->getOutput().addTimestamp<SimulationTimestampJobSubmitted>(
                      new SimulationTimestampJobSubmitted(task));

              // creating job for execution
              job = (WorkflowJob *) this->getJobManager()->createStandardJob(task, file_locations);

            }
            WRENCH_INFO("Scheduling task: %s", task->getID().c_str());
            job->pushCallbackMailbox(this->monitor_callback_mailbox);
            this->getJobManager()->submitJob(job, htcondor_service);
            // create job scheduled event
            this->simulation->getOutput().addTimestamp<SimulationTimestampJobScheduled>(
                    new SimulationTimestampJobScheduled(task));
            WRENCH_INFO("Scheduled task: %s", task->getID().c_str());
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
        void DAGManScheduler::setSimuation(wrench::Simulation *simulation) {
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
