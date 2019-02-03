/**
 * Copyright (c) 2017-2018. The WRENCH Team.
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
#include "wrench/services/compute/htcondor/HTCondorService.h"

namespace wrench {
    namespace pegasus {

        /**
         *  @brief An implementation of the DAGMan meta-scheduler for HTCondor services
         */
        class DAGMan : public WMS {

        public:
            DAGMan(const std::string &hostname,
                   const std::set<HTCondorService *> &htcondor_services,
                   const std::set<StorageService *> &storage_services,
                   FileRegistryService *file_registry_service);

            void setExecutionHosts(const std::vector<std::string> &execution_hosts);

        protected:
            /***********************/
            /** \cond DEVELOPER    */
            /***********************/

            void processEventStandardJobFailure(std::unique_ptr<StandardJobFailedEvent>) override;

            /***********************/
            /** \endcond           */
            /***********************/

        private:
            int main() override;

            struct TaskPriorityComparator {
                bool operator()(WorkflowTask *&lhs, WorkflowTask *&rhs);
            };

            std::string getTaskIDType(const std::string &taskID);

            /** @brief The job manager */
            std::shared_ptr<JobManager> job_manager;
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
        };
    }
}

#endif //WRENCH_PEGASUS_DAGMAN_H
