//
// Created by james oeth on 10/20/17.
//

#include "../include/DagMan.h"
#include <iostream>
#include <wrench-dev.h>


//XBT_LOG_NEW_DEFAULT_CATEGORY(DagMan, "Log category for Simple WMS");

namespace wrench {

    /**
     * @brief Create a Simple WMS with a workflow instance and a scheduler implementation
     *
     * @param workflow: a workflow to execute
     * @param scheduler: a scheduler implementation
     * @param hostname: the name of the host on which to start the WMS
     */



    DagMan::DagMan(Workflow *workflow,
                         std::unique_ptr<Scheduler> scheduler,
                         std::string hostname) : WMS(workflow,
                                                     std::move(scheduler),
                                                     hostname,
                                                     "dagman") {}
    /**
     * @brief main method of the SimpleWMS daemon
     *
     * @return 0 on completion
     *
     * @throw std::runtime_error
     */
    int DagMan::main() {

        return 0;
    }

    /**
     * @brief Process a WorkflowExecutionEvent::STANDARD_JOB_FAILURE
     *
     * @param event: a workflow execution event
     */
    void DagMan::processEventStandardJobFailure(std::unique_ptr<WorkflowExecutionEvent> event) {

    }

    void DagMan::processEventUnsupportedJobType(std::unique_ptr<WorkflowExecutionEvent>) {

    }

    void DagMan::processEventStandardJobCompletion(std::unique_ptr<WorkflowExecutionEvent>) {

    }

    void DagMan::processEventPilotJobStart(std::unique_ptr<WorkflowExecutionEvent>) {

    }

    void DagMan::processEventPilotJobExpiration(std::unique_ptr<WorkflowExecutionEvent>) {

    }

    void DagMan::processEventFileCopyCompletion(std::unique_ptr<WorkflowExecutionEvent>) {

    }

    void DagMan::processEventFileCopyFailure(std::unique_ptr<WorkflowExecutionEvent>) {

    }
}
