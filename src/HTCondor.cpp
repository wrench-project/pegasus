//
// Created by james oeth on 10/20/17.
//

#include "../include/HTCondor.h"

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
