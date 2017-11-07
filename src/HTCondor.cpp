/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "HTCondor.h"

//XBT_LOG_NEW_DEFAULT_CATEGORY(HTCondor, "Log category for Random Scheduler");


using namespace wrench;


void HTCondor::scheduleTasks(JobManager *job_manager,
                                    std::map<std::string, std::vector<wrench::WorkflowTask *>> ready_tasks,
                                    const std::set<ComputeService *> &compute_services) {

}

void HTCondor::schedulePilotJobs(JobManager *job_manager,
                       Workflow *workflow,
                       double pilot_job_duration,
                       const std::set<ComputeService *> &compute_services){

}
