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
                   const std::set<StorageService *> &storage_services);

        protected:
            /***********************/
            /** \cond DEVELOPER    */
            /***********************/

            void processEventStandardJobFailure(std::unique_ptr<WorkflowExecutionEvent>) override;

            /***********************/
            /** \endcond           */
            /***********************/

        private:
            int main() override;

            /** @brief The job manager */
            std::unique_ptr<JobManager> job_manager;
            /** @brief Whether the workflow execution should be aborted */
            bool abort = false;

        };
    }
}

#endif //WRENCH_PEGASUS_DAGMAN_H
