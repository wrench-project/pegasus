/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "DAGMan.h"
#include "HTCondorSchedd.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(DAGMan, "Log category for DAGMan");

namespace wrench {
    namespace pegasus {

        /**
         * @brief Create a DAGMan with a workflow instance, a list of compute services
         *
         * @param hostname: the name of the host on which to start the WMS
         * @param htcondor_services: a set of HTCondor services available to run jobs
         * @param storage_services: a set of storage services available to the WMS
         */
        DAGMan::DAGMan(const std::string &hostname,
                       const std::set<HTCondorService *> &htcondor_services,
                       const std::set<StorageService *> &storage_services,
                       FileRegistryService *file_registry_service) :
                WMS(nullptr, nullptr, (std::set<ComputeService *> &) htcondor_services,
                    storage_services, {}, file_registry_service, hostname, "dagman") {

          this->standard_job_scheduler = std::unique_ptr<StandardJobScheduler>(new HTCondorSchedd());
        }

        /**
         * @brief main method of the DAGMan daemon
         *
         * @return 0 on completion
         *
         * @throw std::runtime_error
         */
        int DAGMan::main() {
          TerminalOutput::setThisProcessLoggingColor(WRENCH_LOGGING_COLOR_GREEN);

          // Check whether the DAGMan has a deferred start time
          checkDeferredStart();

          WRENCH_INFO("Starting DAGMan on host %s listening on mailbox_name %s",
                      S4U_Simulation::getHostName().c_str(),
                      this->mailbox_name.c_str());
          WRENCH_INFO("DAGMan is about to execute a workflow with %lu tasks", this->workflow->getNumberOfTasks());

          // Create a job manager
          this->job_manager = this->createJobManager();

          while (true) {

            // Get the ready tasks
            std::map<std::string, std::vector<WorkflowTask *>> ready_tasks = this->workflow->getReadyTasks();

            // Get the available compute services
            std::set<ComputeService *> htcondor_services = this->getAvailableComputeServices();

            if (htcondor_services.empty()) {
              WRENCH_INFO("Aborting - No HTCondor services available!");
              break;
            }

            // Submit pilot jobs
            if (this->pilot_job_scheduler) {
              WRENCH_INFO("Scheduling pilot jobs...");
              this->pilot_job_scheduler->schedulePilotJobs(htcondor_services);
            }

            // Run ready tasks with defined scheduler implementation
            WRENCH_INFO("Scheduling tasks...");
            this->standard_job_scheduler->scheduleTasks(htcondor_services, ready_tasks);

            // Wait for a workflow execution event, and process it
            try {
              this->waitForAndProcessNextEvent();
            } catch (WorkflowExecutionException &e) {
              WRENCH_INFO("Error while getting next execution event (%s)... ignoring and trying again",
                          (e.getCause()->toString().c_str()));
              continue;
            }

            if (this->abort || workflow->isDone()) {
              break;
            }
          }

          WRENCH_INFO("--------------------------------------------------------");
          if (workflow->isDone()) {
            WRENCH_INFO("Workflow execution is complete!");
          } else {
            WRENCH_INFO("Workflow execution is incomplete!");
          }

          WRENCH_INFO("DAGMan Daemon started on host %s terminating", S4U_Simulation::getHostName().c_str());

          this->job_manager.reset();

          return 0;
        }

        /**
         * @brief Process a WorkflowExecutionEvent::STANDARD_JOB_FAILURE
         *
         * @param event: a workflow execution event
         */
        void DAGMan::processEventStandardJobFailure(std::unique_ptr<WorkflowExecutionEvent> event) {
          auto job = (StandardJob *) (event->job);
          WRENCH_INFO("Notified that a standard job has failed (all its tasks are back in the ready state)");
          WRENCH_INFO("CauseType: %s", event->failure_cause->toString().c_str());
          this->job_manager->forgetJob(job);
          // TODO: retry tasks
          WRENCH_INFO("As a SimpleWMS, I abort as soon as there is a failure");
          this->abort = true;
        }
    }
}
