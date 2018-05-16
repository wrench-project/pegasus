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
            /***********************/
            /** \cond DEVELOPER    */
            /***********************/

            void scheduleTasks(const std::set<wrench::ComputeService *> &compute_services,
                               const std::vector<wrench::WorkflowTask *> &tasks) override;

            int getJobNums(std::vector<unsigned long> num);

            int getNumAvailiableCores(std::vector<unsigned long> num);

            /***********************/
            /** \endcond           */
            /***********************/
        };

    }
}

#endif //WRENCH_PEGASUS_SCHEDD_H
