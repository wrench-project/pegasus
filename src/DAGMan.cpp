/**
 * Copyright (c) 2017-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "DAGMan.h"
#include "DAGManScheduler.h"
#include "PegasusSimulationTimestampTypes.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(DAGMan, "Log category for DAGMan");

namespace wrench {
    namespace pegasus {

        /**
         * @brief Create a DAGMan with a workflow instance, a list of compute services
         *
         * @param hostname: the name of the host on which to start the WMS
         * @param htcondor_services: a set of HTCondor services available to run jobs
         * @param storage_services: a set of storage services available to the WMS
         * @param file_registry_service:
         */
        DAGMan::DAGMan(const std::string &hostname,
                       const std::set<std::shared_ptr<HTCondorComputeService>> &htcondor_services,
                       const std::set<std::shared_ptr<StorageService>> &storage_services,
                       std::shared_ptr<FileRegistryService> &file_registry_service,
                       std::string energy_scheme) :
                WMS(std::unique_ptr<StandardJobScheduler>(
                        new DAGManScheduler(file_registry_service, storage_services)),
                    nullptr,
                    (std::set<std::shared_ptr<ComputeService>> &) htcondor_services,
                    storage_services, {}, file_registry_service, hostname, "dagman"),
                energy_scheme(energy_scheme) {
            // DAGMan performs BFS search by default
            this->running_tasks_level = std::make_pair(0, 0);
        }

        /**
         * @brief Set the list of execution hosts available for computing tasks
         * @param execution_hosts: A vector of execution hosts
         */
        void DAGMan::setExecutionHosts(const std::vector<std::string> &execution_hosts) {
            this->execution_hosts = execution_hosts;
        }

        /**
         * @brief Compare the priority between two workflow tasks
         *
         * @param lhs: pointer to a workflow task
         * @param rhs: pointer to a workflow task
         *
         * @return whether the priority of the left-hand-side workflow tasks is higher
         */
        bool DAGMan::TaskPriorityComparator::operator()(WorkflowTask *&lhs, WorkflowTask *&rhs) {
            return lhs->getPriority() > rhs->getPriority();
        }

        /**
         * @brief main method of the DAGMan daemon
         *
         * @return 0 on completion
         *
         * @throw std::runtime_error
         */
        int DAGMan::main() {
            TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_GREEN);

            // Check whether the DAGMan has a deferred start time
            checkDeferredStart();

            WRENCH_INFO("Starting DAGMan on host %s listening on mailbox_name %s",
                        S4U_Simulation::getHostName().c_str(),
                        this->mailbox_name.c_str());WRENCH_INFO("DAGMan is about to execute a workflow with %lu tasks",
                                                                this->getWorkflow()->getNumberOfTasks());

            // starting monitor
            this->dagman_monitor = std::make_shared<DAGManMonitor>(this->hostname, this->getWorkflow());
            this->dagman_monitor->simulation = this->simulation;
            this->dagman_monitor->start(dagman_monitor, true, true);

            if (not this->energy_scheme.empty()) {
                // create the energy meter
                auto power_meter = this->createPowerMeter(this->execution_hosts, 1);
            }

            // Create a job manager
            this->job_manager = this->createJobManager();
            this->data_movement_manager = this->createDataMovementManager();

            // scheduler
            auto dagman_scheduler = (DAGManScheduler *) this->getStandardJobScheduler();
            dagman_scheduler->setSimulation(this->simulation);
            dagman_scheduler->setDataMovementManager(data_movement_manager);
            dagman_scheduler->setMonitorCallbackMailbox(this->dagman_monitor->getMailbox());

            WRENCH_INFO("Sleeping for 3 seconds to ensure ProcessId uniqueness (DAGMan simulated waiting time)");
            Simulation::sleep(3.0);WRENCH_INFO("Bootstrapping...");

            while (true) {
                // DAGMan only runs tasks up to the current level
                std::vector<WorkflowTask *> dagman_ready_tasks;

                for (auto task : this->getWorkflow()->getReadyTasks()) {
                    // set task priority according to DAGMan rules for parent tasks
                    long max_priority = task->getPriority();
                    for (auto parent : task->getWorkflow()->getTaskParents(task)) {
                        if (parent->getPriority() > max_priority) {
                            max_priority = parent->getPriority();
                        }
                    }
                    task->setPriority(max_priority);
                    dagman_ready_tasks.push_back(task);
                }

                // sort tasks by priority
                std::sort(dagman_ready_tasks.begin(), dagman_ready_tasks.end(), TaskPriorityComparator());

                unsigned long submitted_tasks = 0;

                std::vector<WorkflowTask *> tasks_to_submit;

                for (auto it = dagman_ready_tasks.begin(); it != dagman_ready_tasks.end();) {
                    auto task = *it;

                    if (tasks_to_submit.size() == 5) {
                        break;
                    }

                    // get task ID type
                    std::string task_id_type = this->getTaskIDType(task->getID());

                    if (this->current_running_task_type.first.empty() ||
                        this->current_running_task_type.first == task_id_type) {
                        // by default DAGMan only runs a single register job at once
                        if (task->getID().find("register_local") == 0) {
                            if (running_register_tasks > 0) {
                                submitted_tasks++;
                                dagman_ready_tasks.erase(it);
                                continue;
                            }
                            running_register_tasks++;
                        }

                        if (this->scheduled_tasks.find(task) == this->scheduled_tasks.end()) {
                            // update current running task type
                            if (this->current_running_task_type.first.empty()) {
                                this->current_running_task_type = std::make_pair(task_id_type, 1);
                            } else {
                                this->current_running_task_type.second++;
                            }

                            this->scheduled_tasks.insert(task);
                            tasks_to_submit.push_back(task);
                            dagman_ready_tasks.erase(it);

                            // updating number of tasks running per level
                            if (task->getTopLevel() > this->running_tasks_level.first) {
                                this->running_tasks_level = std::make_pair(task->getTopLevel(), 1);

                            } else if (task->getTopLevel() == this->running_tasks_level.first) {
                                this->running_tasks_level.second++;
                            }

                            // create job submitted event
                            this->simulation->getOutput().addTimestamp<SimulationTimestampJobSubmitted>(
                                    new SimulationTimestampJobSubmitted(task));WRENCH_INFO("Submitted task: %s",
                                                                                           task->getID().c_str());

                        } else {
                            submitted_tasks++;
                            dagman_ready_tasks.erase(it);
                        }
                    } else {
                        submitted_tasks++;
                        dagman_ready_tasks.erase(it);
                    }
                }

                // submit tasks
                if (not tasks_to_submit.empty()) {
                    // Get the available compute services
                    auto htcondor_services = this->getAvailableComputeServices<ComputeService>();

                    if (htcondor_services.empty()) { WRENCH_INFO("Aborting - No HTCondor services available!");
                        break;
                    }

                    // Submit pilot jobs
                    if (this->getPilotJobScheduler()) { WRENCH_INFO("Scheduling pilot jobs...");
                        this->getPilotJobScheduler()->schedulePilotJobs(htcondor_services);
                    }

                    // Run ready tasks with defined scheduler implementation
                    WRENCH_INFO("Scheduling tasks...");
                    this->getStandardJobScheduler()->scheduleTasks(htcondor_services, tasks_to_submit);
                }

                // simulate timespan between DAGMan status pull for HTCondor
                Simulation::sleep(0.1);
                for (auto job : this->dagman_monitor->getCompletedJobs()) {
                    auto standard_job = (StandardJob *) job;
                    for (auto task : standard_job->getTasks()) { WRENCH_INFO("    Task completed: %s",
                                                                             task->getID().c_str());

                        // update current running task ID type
                        this->current_running_task_type.second -= 1;
                        if (this->current_running_task_type.second == 0) {
                            this->current_running_task_type.first = "";
                        }

                        if (task->getID().find("register_") == 0) {
                            // a register task has completed
                            this->running_register_tasks--;
                        }

                        // notify a task in a specific level has completed
                        if (task->getTopLevel() > this->running_tasks_level.first) {
                            throw std::invalid_argument(
                                    "DAGMan::processEventStandardJobCompletion: Invalid task level");
                        }
                        if (task->getTopLevel() == this->running_tasks_level.first) {
                            this->running_tasks_level.second--;
                        }

                        // create job completion event
                        this->simulation->getOutput().addTimestamp<SimulationTimestampJobCompletion>(
                                new SimulationTimestampJobCompletion(task));
                    }
                }

                if (this->abort || this->getWorkflow()->isDone()) {
                    break;
                }
            }

            WRENCH_INFO("--------------------------------------------------------");
            if (this->getWorkflow()->isDone()) { WRENCH_INFO("Workflow execution is complete!");
            } else { WRENCH_INFO("Workflow execution is incomplete!");
            }

            WRENCH_INFO("DAGMan Daemon started on host %s terminating", S4U_Simulation::getHostName().c_str());

            this->job_manager.reset();
            this->data_movement_manager.reset();

            return 0;
        }

        /**
         * @brief Extract the task ID type from a task ID
         *
         * @param taskID: task ID
         * @return Task ID type
         */
        std::string DAGMan::getTaskIDType(const std::string &taskID) {
            return taskID.substr(0, taskID.find('_'));
        }

        /**
         * @brief Instantiate and start a power meter
         * @param hostname_list: the list of metered hosts, as hostnames
         * @param measurement_period: the measurement period
         * @return a power meter
         */
        std::shared_ptr<PowerMeter> DAGMan::createPowerMeter(const std::vector<std::string> &hostname_list,
                                                             double measurement_period) {
            auto power_meter_raw_ptr = new PowerMeter(this, hostname_list, measurement_period,
                                                      this->energy_scheme == "pairwise");
            std::shared_ptr<PowerMeter> power_meter = std::shared_ptr<PowerMeter>(power_meter_raw_ptr);
            power_meter->simulation = this->simulation;
            power_meter->start(power_meter, true, true); // Always daemonize
            return power_meter;
        }

        /**
         * @brief Process a WorkflowExecutionEvent::STANDARD_JOB_FAILURE
         *
         * @param event: a workflow execution event
         */
        void DAGMan::processEventStandardJobFailure(std::shared_ptr<StandardJobFailedEvent> event) {
            auto job = event->standard_job;

            WRENCH_INFO("Notified that a standard job has failed (all its tasks are back in the ready state)");

            WRENCH_INFO("CauseType: %s", event->failure_cause->toString().c_str());

            this->job_manager->forgetJob(job);

            for (auto task : job->getTasks()) {
                this->scheduled_tasks.erase(task);
            }

            // TODO: retry tasks
            WRENCH_INFO("As a SimpleWMS, I abort as soon as there is a failure");
            this->abort = true;
        }
    }
}
