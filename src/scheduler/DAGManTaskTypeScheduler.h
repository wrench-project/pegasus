/**
 * Copyright (c) 2017-2019. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef WRENCH_DAGMAN_TASK_TYPE_SCHEDD_H
#define WRENCH_DAGMAN_TASK_TYPE_SCHEDD_H

#include <vector>
#include <wrench-dev.h>
#include "scheduler/DAGManScheduler.h"

namespace wrench {

    class Simulation;

    namespace pegasus {

        /**
         * @brief HTCondor scheduler daemon
         */
        class DAGManTaskTypeScheduler : public DAGManScheduler {

        public:
            DAGManTaskTypeScheduler(FileRegistryService *file_registry_service,
                                    const std::set<StorageService *> &storage_services);

            ~DAGManTaskTypeScheduler();

            /***********************/
            /** \cond DEVELOPER    */
            /***********************/

            void scheduleTasks(const std::set<wrench::ComputeService *> &compute_services,
                               const std::vector<wrench::WorkflowTask *> &tasks) override;

            /***********************/
            /** \endcond           */
            /***********************/
        };
    }
}

#endif //WRENCH_DAGMAN_TASK_TYPE_SCHEDD_H
