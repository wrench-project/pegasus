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
        HTCondorSchedd::HTCondorSchedd(FileRegistryService *file_registry_service,
                                       const std::set<StorageService *> &storage_services)
                : file_registry_service(file_registry_service), storage_services(storage_services) {

          // DAGMan performs BFS search by default
          this->running_tasks_level = std::make_pair(0, 0);
        }

        /**
         * @brief Destructor
         */
        HTCondorSchedd::~HTCondorSchedd() {
          this->pending_tasks.clear();
        }

        /**
         * @brief Compare the priority between two workflow tasks
         *
         * @param lhs: pointer to a workflow task
         * @param rhs: pointer to a workflow task
         *
         * @return whether the priority of the left-hand-side workflow tasks is higher
         */
        bool HTCondorSchedd::TaskPriorityComparator::operator()(WorkflowTask *&lhs, WorkflowTask *&rhs) {
          return lhs->getPriority() > rhs->getPriority();
        }

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
          std::vector<WorkflowTask *> tasks_to_append;

          std::vector<unsigned long> idle_cores = htcondor_service->getNumIdleCores();

          for (auto task : tasks) {
            if (task->getTopLevel() <= this->running_tasks_level.first ||
                (task->getTopLevel() == this->running_tasks_level.first + 1 &&
                 this->running_tasks_level.second == 0)) {

              if (std::find(tasks_to_append.begin(), tasks_to_append.end(), task) == tasks_to_append.end()) {

                long max_priority = task->getPriority();
                for (auto parent : task->getWorkflow()->getTaskParents(task)) {
                  if (parent->getPriority() > max_priority) {
                    max_priority = parent->getPriority();
                  }
                }
                task->setPriority(max_priority);
                tasks_to_append.push_back(task);
              }
            }
          }

          // sort tasks to append by priority
          std::sort(tasks_to_append.begin(), tasks_to_append.end(), TaskPriorityComparator());

          while (not tasks_to_append.empty()) {

            unsigned long appended_tasks = 0;
            for (auto it = tasks_to_append.begin(); it != tasks_to_append.end();) {
              auto task = *it;
              if (appended_tasks == 5) {
                break;
              }
              if (std::find(this->pending_tasks.begin(), this->pending_tasks.end(), task) ==
                  this->pending_tasks.end()) {
                this->pending_tasks.push_back(task);
                appended_tasks++;
                // create job start event
                this->simulation->getOutput().addTimestamp<SimulationTimestampJobSubmitted>(
                        new SimulationTimestampJobSubmitted(task));
              }
              tasks_to_append.erase(it);
            }

            // sort by task priority
            std::sort(this->pending_tasks.begin(), this->pending_tasks.end(), TaskPriorityComparator());

            for (auto it = this->pending_tasks.begin(); it != this->pending_tasks.end();) {
              auto task = *it;
              bool scheduled = false;

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
                scheduled = true;

              } else {
                // regular compute task
                if (task->getID().find("register_local") == 0 && running_register_tasks > 0) {
                  ++it;
                  continue;
                }

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

                    // create job start event
                    this->simulation->getOutput().addTimestamp<SimulationTimestampJobSubmitted>(
                            new SimulationTimestampJobSubmitted(task));

                    // creating job for execution
                    auto *job = (WorkflowJob *) this->getJobManager()->createStandardJob(task, file_locations);
                    this->getJobManager()->submitJob(job, htcondor_service);

                    if (task->getID().find("register_local") == 0) {
                      running_register_tasks++;
                    }

                    idle_core -= task->getMinNumCores();
                    scheduled = true;
                    break;
                  }
                }
              }
              if (scheduled) {
                if (task->getTopLevel() > this->running_tasks_level.first) {
                  this->running_tasks_level = std::make_pair(task->getTopLevel(), 1);

                } else if (task->getTopLevel() == this->running_tasks_level.first) {
                  this->running_tasks_level.second++;
                }

                scheduled_tasks++;
                this->pending_tasks.erase(it);
              } else {
                ++it;
              }
            }

            if (appended_tasks == 5) {
              // sleeping 5 seconds between task submissions in DAGMan
              Simulation::sleep(5.0);
            }
          }

          WRENCH_INFO("Done with scheduling tasks as standard jobs: %ld tasks scheduled out of %ld", scheduled_tasks,
                      tasks.size());
        }

        /**
         * @brief
         *
         * @param simulation: a pointer to the simulation object
         */
        void HTCondorSchedd::setSimuation(wrench::Simulation *simulation) {
          this->simulation = simulation;
        }

        /**
         * @brief Notify a task in a specific level has completed
         *
         * @param level: task level
         *
         * @throw std::invalid_argument
         */
        void HTCondorSchedd::notifyRunningTaskLevelCompletion(const unsigned long level) {
          if (level > this->running_tasks_level.first) {
            throw std::invalid_argument("HTCondorSchedd::notifyRunningTaskLevelCompletion: Invalid level");
          }
          if (level == this->running_tasks_level.first) {
            this->running_tasks_level.second--;
          }
        }

        /**
         * @brief Notify a register task has completed
         */
        void HTCondorSchedd::notifyRegisterTaskCompletion() {
          this->running_register_tasks--;
        }
    }
}
