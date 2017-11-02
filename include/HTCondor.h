//
// Created by james oeth on 10/20/17.
//

//manages resource pool and receives jobs from dagman

#ifndef PROJECT_HTCONDOR_H
#define PROJECT_HTCONDOR_H

#include "wrench.h"

namespace wrench {

    /**
     * @brief A max-min Scheduler
     */
    class HTCondor : public Scheduler {


    public:

        /***********************/
        /** \cond DEVELOPER    */
        /***********************/

        void scheduleTasks(JobManager *job_manager,
                           std::map<std::string, std::vector<WorkflowTask *>> ready_tasks,
                           const std::set<ComputeService *> &compute_services);

    private:
        struct MaxMinComparator {
            bool operator()(std::pair<std::string, std::vector<WorkflowTask *>> &lhs,
                            std::pair<std::string, std::vector<WorkflowTask *>> &rhs);
        };
        /***********************/
        /** \endcond           */
        /***********************/
    };


}
#endif //PROJECT_HTCONDOR_H
