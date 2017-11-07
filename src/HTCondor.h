/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

//manages resource pool and receives jobs from dagman

#ifndef PROJECT_HTCONDOR_H
#define PROJECT_HTCONDOR_H

#include <wrench-dev.h>



using namespace wrench;

    /**
     * @brief cloud scheduler for htcondor
     */
    class HTCondor : public Scheduler {
        /**
            * @brief A random Scheduler
            */
            /***********************/
            /** \cond DEVELOPER    */
            /***********************/
        public:
            void scheduleTasks(JobManager *job_manager,
                               std::map<std::string, std::vector<wrench::WorkflowTask *>> ready_tasks,
                               const std::set<ComputeService *> &compute_services);

            void schedulePilotJobs(JobManager *job_manager,
                               Workflow *workflow,
                               double pilot_job_duration,
                               const std::set<ComputeService *> &compute_services);




            /***********************/
            /** \endcond           */
            /***********************/
        };

#endif //PROJECT_HTCONDOR_H
