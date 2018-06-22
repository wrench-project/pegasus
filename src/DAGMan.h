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
#include "HTCondorService.h"

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

        protected:
            /***********************/
            /** \cond DEVELOPER    */
            /***********************/

            void processEventStandardJobCompletion(std::unique_ptr<StandardJobCompletedEvent> event) override;

            void processEventStandardJobFailure(std::unique_ptr<StandardJobFailedEvent>) override;

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
            /** @brief Whether the workflow execution should be aborted */
            bool abort = false;
            /** @brief Pair of level and number of running tasks in the level */
            std::pair<unsigned long, unsigned long> running_tasks_level;
            /** @brief */
            unsigned long running_register_tasks = 0;
            /** @brief */
            std::set<WorkflowTask *> scheduled_tasks;
        };
    }
}

#endif //WRENCH_PEGASUS_DAGMAN_H
