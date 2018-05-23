/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "HTCondorSchedd.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(HTCondorSchedd, "Log category for HTCondor Scheduler Daemon");

namespace wrench {
    namespace pegasus {

        /**
         * @brief Constructor
         *
         * @param file_registry_service: a pointer to the file registry service
         * @param default_storage_service: a pointer to a storage services used for storing intermediate data
         */
        HTCondorSchedd::HTCondorSchedd(wrench::FileRegistryService *file_registry_service,
                                       StorageService *default_storage_service)
                : file_registry_service(file_registry_service), default_storage_service(default_storage_service) {}

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
          auto htcondor_service = *(compute_services.begin());

          unsigned long scheduled_tasks = 0;
          std::vector<unsigned long> idle_cores = htcondor_service->getNumIdleCores();

          for (auto task : tasks) {
            for (unsigned long &idle_core : idle_cores) {
              if (task->getMinNumCores() <= idle_core) {

                // input/output files
                std::map<WorkflowFile *, StorageService *> file_locations;
                for (auto f : task->getInputFiles()) {
                  file_locations.insert(std::make_pair(f, default_storage_service));
                }
                for (auto f : task->getOutputFiles()) {
                  file_locations.insert(std::make_pair(f, default_storage_service));
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

          WRENCH_INFO("Done with scheduling tasks as standard jobs: %ld tasks scheduled out of %ld", scheduled_tasks,
                      tasks.size());
        }
    }
}
