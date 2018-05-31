/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "HTCondorSchedd.h"
#include "HTCondorService.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(HTCondorSchedd, "Log category for HTCondor Scheduler Daemon");

namespace wrench {
    namespace pegasus {

        /**
         * @brief Constructor
         *
         * @param file_registry_service: a pointer to the file registry service
         * @param storage_services:
         */
        HTCondorSchedd::HTCondorSchedd(FileRegistryService *file_registry_service,
                                       const std::set<StorageService *> &storage_services)
                : file_registry_service(file_registry_service), storage_services(storage_services) {}

        /**
         * @brief Schedule and run a set of ready tasks on available HTCondor resources
         *
         * @param compute_services: a set of compute services available to run jobs
         * @param tasks: a map of (ready) workflow tasks
         *
         * @throw std::runtime_error
         */
        void HTCondorSchedd::scheduleTasks(const std::set<ComputeService *> &compute_services,
                                           const std::vector<WorkflowTask *> &tasks) {

          WRENCH_INFO("There are %ld ready tasks to schedule", tasks.size());

          // TODO: select htcondor service based on condor queue name
          auto htcondor_service = (HTCondorService *) *(compute_services.begin());

          unsigned long scheduled_tasks = 0;
          std::vector<unsigned long> idle_cores = htcondor_service->getNumIdleCores();

          for (auto task : tasks) {

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
              WorkflowJob *job = (WorkflowJob *) this->getJobManager()->createStandardJob({task},
                                                                                          file_locations,
                                                                                          pre_file_copies,
                                                                                          post_file_copies, {});
              this->getJobManager()->submitJob(job, htcondor_service);
              scheduled_tasks++;

            } else {
              // regular compute task

              for (unsigned long &idle_core : idle_cores) {
                if (task->getMinNumCores() <= idle_core) {

                  // input/output files
                  std::map<WorkflowFile *, StorageService *> file_locations;
                  for (auto f : task->getInputFiles()) {
                    file_locations.insert(std::make_pair(f, htcondor_service->getLocalStorageService()));
                  }
                  for (auto f : task->getOutputFiles()) {
                    file_locations.insert(std::make_pair(f, htcondor_service->getLocalStorageService()));
                  }

                  // creating job for execution
                  WorkflowJob *job = (WorkflowJob *)
                          this->getJobManager()->createStandardJob(task, file_locations);
                  this->getJobManager()->submitJob(job, htcondor_service);

                  scheduled_tasks++;
                  idle_core -= task->getMinNumCores();
                  break;
                }
              }
            }
          }

          WRENCH_INFO("Done with scheduling tasks as standard jobs: %ld tasks scheduled out of %ld", scheduled_tasks,
                      tasks.size());
        }
    }
}
