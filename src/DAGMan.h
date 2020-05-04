/**
 * Copyright (c) 2017-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef WRENCH_PEGASUS_DAGMAN_H
#define WRENCH_PEGASUS_DAGMAN_H

#include <wrench-dev.h>
#include "DAGManMonitor.h"
#include "PowerMeter.h"

namespace wrench {
    namespace pegasus {

        /**
         *  @brief An implementation of the DAGMan meta-scheduler for HTCondor services
         */
        class DAGMan : public WMS {
        public:
            DAGMan(const std::string &hostname,
                   const std::set<std::shared_ptr<HTCondorComputeService>> &htcondor_services,
                   const std::set<std::shared_ptr<StorageService>> &storage_services,
                   std::shared_ptr<FileRegistryService> &file_registry_service,
                   std::string energy_scheme = "");

            void setExecutionHosts(const std::vector<std::string> &execution_hosts);

        protected:
            /***********************/
            /** \cond DEVELOPER    */
            /***********************/

            void processEventStandardJobFailure(std::shared_ptr<StandardJobFailedEvent>) override;

            std::string getTaskIDType(const std::string &taskID);

            std::shared_ptr<PowerMeter> createPowerMeter(const std::vector<std::string> &hostname_list,
                                                         double measurement_period);

            /***********************/
            /** \endcond           */
            /***********************/

        private:
            int main() override;

            struct TaskPriorityComparator {
                bool operator()(WorkflowTask *&lhs, WorkflowTask *&rhs);
            };

            /** @brief The job manager */
            std::shared_ptr<JobManager> job_manager;
            /** @brief The data movement manager */
            std::shared_ptr<DataMovementManager> data_movement_manager;
            /** @brief Whether the workflow execution should be aborted */
            bool abort = false;
            /** @brief Pair of level and number of running tasks in the level */
            std::pair<unsigned long, unsigned long> running_tasks_level;
            /** @brief */
            unsigned long running_register_tasks = 0;
            /** @brief Set of tasks scheduled for running */
            std::set<WorkflowTask *> scheduled_tasks;
            /** @brief Pair of current running task transformation type */
            std::pair<std::string, int> current_running_task_type;
            /** @brief */
            std::shared_ptr<DAGManMonitor> dagman_monitor;
            /** @brief List of execution hosts */
            std::vector<std::string> execution_hosts;
            /** @brief Energy scheme (if provided) */
            std::string energy_scheme;
        };
    }
}

#endif //WRENCH_PEGASUS_DAGMAN_H
