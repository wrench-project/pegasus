/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "HTCondorSchedd.h"


XBT_LOG_NEW_DEFAULT_CATEGORY(HTCondorSchedd, "Log category for HTCondor Scheduler Daemon");

namespace wrench {
    namespace pegasus {

        /**
         * @brief Schedule and run a set of ready tasks on available HTCondor resources
         *
         * @param compute_services: a set of compute services available to run jobs
         * @param tasks: a map of (ready) workflow tasks
         *
         * @throw std::runtime_error
         */
        void HTCondorSchedd::scheduleTasks(const std::set<ComputeService *> &compute_services,
                                           const std::map<std::string, std::vector<WorkflowTask *>> &tasks) {

          WRENCH_INFO("There are %ld ready tasks to schedule", tasks.size());

          for (auto itc : tasks) {
            // TODO add support to pilot jobs

            WorkflowJob *job = (WorkflowJob *) job_manager->createStandardJob(itc.second, {});
            job_manager->submitJob(job, *compute_services.begin());
          }
          WRENCH_INFO("Done with scheduling tasks as standard jobs");
        }
    }
}
