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

    class Simulation;

    namespace pegasus {

        /**
         * @brief HTCondor scheduler daemon
         */
        class DAGManScheduler : public StandardJobScheduler {

        public:
            DAGManScheduler(FileRegistryService *file_registry_service,
                           const std::set<StorageService *> &storage_services);

            ~DAGManScheduler();

            /***********************/
            /** \cond DEVELOPER    */
            /***********************/

            void scheduleTasks(const std::set<wrench::ComputeService *> &compute_services,
                               const std::vector<wrench::WorkflowTask *> &tasks) override;

            void setSimuation(Simulation *simulation);

            /***********************/
            /** \endcond           */
            /***********************/

        private:
            /** @brief The file registry service */
            FileRegistryService *file_registry_service;
            /** @brief */
            std::set<StorageService *> storage_services;
            /** @brief */
            Simulation *simulation;
        };

    }
}

#endif //WRENCH_PEGASUS_SCHEDD_H