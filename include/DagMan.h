//
// Created by james oeth on 10/20/17.
//

//this will look through dag and send jobs to htcondor

#ifndef PROJECT_DAGMAN_H
#define PROJECT_DAGMAN_H

#include <wrench-dev.h>

namespace wrench {

    class DagMan : public WMS {

    public:
        DagMan(Workflow *, std::unique_ptr<Scheduler>, std::string);

    protected:
        /***********************/
        /** \cond DEVELOPER    */
        /***********************/

        void processEventStandardJobFailure(std::unique_ptr<WorkflowExecutionEvent>);

        void processEventUnsupportedJobType(std::unique_ptr<WorkflowExecutionEvent>);

        void processEventStandardJobCompletion(std::unique_ptr<WorkflowExecutionEvent>);

        void processEventPilotJobStart(std::unique_ptr<WorkflowExecutionEvent>);

        void processEventPilotJobExpiration(std::unique_ptr<WorkflowExecutionEvent>);

        void processEventFileCopyCompletion(std::unique_ptr<WorkflowExecutionEvent>);

        void processEventFileCopyFailure(std::unique_ptr<WorkflowExecutionEvent>);

        /***********************/
        /** \endcond           */
        /***********************/

    private:
        int main();

        /** @brief The job manager */
        std::unique_ptr<JobManager> job_manager;
        /** @brief Whether the workflow execution should be aborted */
        bool abort = false;

    };

}
#endif //PROJECT_DAGMAN_H
