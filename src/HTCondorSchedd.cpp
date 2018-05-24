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

          std::set<StandardJob *> transfer_jobs;

          for (auto task : tasks) {

            if (task->getTaskType() == WorkflowTask::TaskType::TRANSFER_IN) {

              // data stage-in
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
                  this->getDataMovementManager()->doSynchronousFileCopy(
                          input_file, src, dest, file_registry_service);
                }

              }
              task->setState(WorkflowTask::State::PENDING);
              transfer_jobs.insert(this->getJobManager()->createStandardJob(task, {}));
              scheduled_tasks++;

            } else if (task->getTaskType() == WorkflowTask::TaskType::TRANSFER_OUT) {
              // data stage-out
              std::cerr << "TASK STAGE OUT: " << task->getTaskType() << std::endl;

            } else {
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
                  WorkflowJob *job = (WorkflowJob *) this->getJobManager()->createStandardJob(task, file_locations);
                  this->getJobManager()->submitJob(job, htcondor_service);

                  scheduled_tasks++;
                  idle_core -= task->getMinNumCores();
                  break;
                }
              }
            }
          }

          // sending completion notification for transfer jobs
          for (auto job : transfer_jobs) {
            for (auto task : job->getTasks()) {
              task->setInternalState(WorkflowTask::InternalState::TASK_COMPLETED);
              for (auto child : task->getWorkflow()->getTaskChildren(task)) {
                child->setInternalState(WorkflowTask::InternalState::TASK_READY);
              }
            }
            try {
              S4U_Mailbox::dputMessage(this->getJobManager()->mailbox_name,
                                       new ComputeServiceStandardJobDoneMessage(job, htcondor_service, 0.0));
            } catch (std::shared_ptr<NetworkError> &cause) {
              // ignore
            }
          }

          WRENCH_INFO("Done with scheduling tasks as standard jobs: %ld tasks scheduled out of %ld", scheduled_tasks,
                      tasks.size());
        }
    }
}
