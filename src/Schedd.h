//
// Created by james oeth on 1/25/18.
//

#ifndef PEGASUS_SCHEDD_H
#define PEGASUS_SCHEDD_H


#include <wrench-dev.h>
#include "wrench/workflow/job/PilotJob.h"
#include "string"
#include <vector>

/**
 * @brief A cloud Scheduler for htcondor
 */
class Schedd : public Scheduler {

public:
    Schedd(ComputeService* cs, std::vector<std::string> &execution_hosts,
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


};

#endif //PEGASUS_SCHEDD_H
