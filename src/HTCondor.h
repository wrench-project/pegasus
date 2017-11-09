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
#include "wrench/workflow/job/PilotJob.h"
#include "string"

using namespace wrench;


    /**
     * @brief A cloud Scheduler for htcondor
     */
class HTCondor : public Scheduler {

public:
    HTCondor(ComputeService *cloud_service, std::vector<std::string> &execution_hosts,
                   wrench::Simulation *simulation);

    /***********************/
    /** \cond DEVELOPER    */
    /***********************/

    void scheduleTasks(JobManager *job_manager,
                       std::map<std::string, std::vector<wrench::WorkflowTask *>> ready_tasks,
                       const std::set<ComputeService *> &compute_services) override;

    /***********************/
    /** \endcond           */
    /***********************/

private:

    std::string choosePMHostname();
    ComputeService *cloud_service;
    std::vector<std::string> execution_hosts;
    std::map<std::string, std::vector<std::string>> vm_list;
    wrench::Simulation *simulation;
};

#endif //PROJECT_HTCONDOR_H
