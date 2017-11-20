/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

//this will look through dag and send jobs to htcondor

#ifndef PROJECT_DAGMAN_H
#define PROJECT_DAGMAN_H

#include <wrench-dev.h>

using namespace wrench;

class Simulation;

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

#endif //PROJECT_DAGMAN_H
