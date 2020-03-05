/**
 * Copyright (c) 2017-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef PEGASUS_DAGMANMONITOR_H
#define PEGASUS_DAGMANMONITOR_H

#include <wrench-dev.h>

namespace wrench {
    namespace pegasus {

        /**
         * @brief A monitor for DAGMan
         */
        class DAGManMonitor : public Service {
        public:
            DAGManMonitor(std::string &hostname, Workflow *workflow);

            ~DAGManMonitor() override;

            const std::string getMailbox();

            std::set<WorkflowJob *> getCompletedJobs();

        private:
            int main() override;

            bool processNextMessage();

            void processStandardJobCompletion(StandardJob *job);

            std::set<WorkflowJob *> completed_jobs;

            Workflow *workflow;
        };
    }
}

#endif //PEGASUS_DAGMANMONITOR_H
