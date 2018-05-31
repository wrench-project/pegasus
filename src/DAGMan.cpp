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
         * @param file_registry_service:
         */
        DAGMan::DAGMan(const std::string &hostname,
                       const std::set<HTCondorService *> &htcondor_services,
                       const std::set<StorageService *> &storage_services,
                       FileRegistryService *file_registry_service) :
                WMS(std::unique_ptr<StandardJobScheduler>(
                        new HTCondorSchedd(file_registry_service, storage_services)),
                    nullptr,
                    (std::set<ComputeService *> &) htcondor_services,
                    storage_services, {}, file_registry_service, hostname, "dagman") {
        }

        /**
         * @brief main method of the DAGMan daemon
         *
         * @return 0 on completion
         *
         * @throw std::runtime_error
         */
        int DAGMan::main() {
          TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_GREEN);

          // Check whether the DAGMan has a deferred start time
          checkDeferredStart();

          WRENCH_INFO("Starting DAGMan on host %s listening on mailbox_name %s", S4U_Simulation::getHostName().c_str(),
                      this->mailbox_name.c_str());
          WRENCH_INFO("DAGMan is about to execute a workflow with %lu tasks", this->getWorkflow()->getNumberOfTasks());

          // Create a job manager
          this->job_manager = this->createJobManager();
          auto data_movement_manager = this->createDataMovementManager();
          this->getStandardJobScheduler()->setDataMovementManager(data_movement_manager.get());

          WRENCH_INFO("Sleeping for 3 seconds to ensure ProcessId uniqueness (DAGMan simulated waiting time)");
          Simulation::sleep(3.0);
          WRENCH_INFO("Bootstrapping...");

          while (true) {
            // Get the ready tasks
            std::vector<WorkflowTask *> ready_tasks = this->getWorkflow()->getReadyTasks();

            if (not ready_tasks.empty()) {
              // Get the available compute services
              std::set<ComputeService *> htcondor_services = this->getAvailableComputeServices();

              if (htcondor_services.empty()) {
                WRENCH_INFO("Aborting - No HTCondor services available!");
                break;
              }

              // Submit pilot jobs
              if (this->getPilotJobScheduler()) {
                WRENCH_INFO("Scheduling pilot jobs...");
                this->getPilotJobScheduler()->schedulePilotJobs(htcondor_services);
              }

              // Run ready tasks with defined scheduler implementation
              WRENCH_INFO("Scheduling tasks...");
              this->getStandardJobScheduler()->scheduleTasks(htcondor_services, ready_tasks);
            }

            // Wait for a workflow execution event, and process it
            try {
              this->waitForAndProcessNextEvent();
            } catch (WorkflowExecutionException &e) {
              WRENCH_INFO("Error while getting next execution event (%s)... ignoring and trying again",
                          (e.getCause()->toString().c_str()));
              continue;
            }

            if (this->abort || this->getWorkflow()->isDone()) {
              break;
            }

            if (not ready_tasks.empty()) {
              WRENCH_INFO("Sleeping for 5 seconds for simulating DAGMan monitoring thread");
              Simulation::sleep(5.0);
            }
          }

          WRENCH_INFO("--------------------------------------------------------");
          if (this->getWorkflow()->isDone()) {
            WRENCH_INFO("Workflow execution is complete!");
          } else {
            WRENCH_INFO("Workflow execution is incomplete!");
          }

          WRENCH_INFO("DAGMan Daemon started on host %s terminating", S4U_Simulation::getHostName().c_str());

          this->job_manager.reset();

          return 0;
        }

        void DAGMan::processEventStandardJobCompletion(std::unique_ptr<StandardJobCompletedEvent> event) {
          auto standard_job = event->standard_job;
          WRENCH_INFO("Notified that a %ld-task job has completed", standard_job->getNumTasks());
        }

        /**
         * @brief Process a WorkflowExecutionEvent::STANDARD_JOB_FAILURE
         *
         * @param event: a workflow execution event
         */
        void DAGMan::processEventStandardJobFailure(std::unique_ptr<StandardJobFailedEvent> event) {
          auto job = event->standard_job;
          WRENCH_INFO("Notified that a standard job has failed (all its tasks are back in the ready state)");
          WRENCH_INFO("CauseType: %s", event->failure_cause->toString().c_str());
          this->job_manager->forgetJob(job);
          // TODO: retry tasks
          WRENCH_INFO("As a SimpleWMS, I abort as soon as there is a failure");
          this->abort = true;
        }
    }
}
