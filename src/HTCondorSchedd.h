/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef WRENCH_PEGASUS_SCHEDD_H
#define WRENCH_PEGASUS_SCHEDD_H

#include <vector>
#include <wrench-dev.h>

namespace wrench {
    namespace pegasus {

        /**
         * @brief HTCondor scheduler daemon
         */
        class HTCondorSchedd : public StandardJobScheduler {

        public:
            HTCondorSchedd(FileRegistryService *file_registry_service,
                           const std::set<StorageService *> &storage_services);

            ~HTCondorSchedd();

            /***********************/
            /** \cond DEVELOPER    */
            /***********************/

            void scheduleTasks(const std::set<wrench::ComputeService *> &compute_services,
                               const std::vector<wrench::WorkflowTask *> &tasks) override;

            void notifyRunningTaskLevelCompletion(unsigned long level);

            void notifyRegisterTaskCompletion();

            /***********************/
            /** \endcond           */
            /***********************/

        private:
            struct TaskPriorityComparator {
                bool operator()(WorkflowTask *&lhs, WorkflowTask *&rhs);
            };

            /** @brief The file registry service */
            FileRegistryService *file_registry_service;
            /** @brief */
            std::set<StorageService *> storage_services;
            /** @brief */
            std::vector<WorkflowTask *> pending_tasks;
            /** @brief Pair of level and number of running tasks in the level */
            std::pair<unsigned long, unsigned long> running_tasks_level;
            /** @brief */
            unsigned long running_register_tasks = 0;
        };

    }
}

#endif //WRENCH_PEGASUS_SCHEDD_H
