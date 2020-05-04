/**
 * Copyright (c) 2017-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef PEGASUS_PEGASUSSIMULATIONTIMESTAMPTYPES_H
#define PEGASUS_PEGASUSSIMULATIONTIMESTAMPTYPES_H

#include <wrench-dev.h>

namespace wrench {

    class WorkflowTask;

    namespace pegasus {

        class SimulationTimestampJobSubmitted {
        public:
            SimulationTimestampJobSubmitted(WorkflowTask *task);

            WorkflowTask *getTask();

            double getClock();

        private:
            double clock;
            WorkflowTask *task;
        };

        class SimulationTimestampJobScheduled {
        public:
            SimulationTimestampJobScheduled(WorkflowTask *task);

            WorkflowTask *getTask();

            double getClock();

        private:
            double clock;
            WorkflowTask *task;
        };

        class SimulationTimestampJobCompletion {
        public:
            SimulationTimestampJobCompletion(WorkflowTask *task);

            WorkflowTask *getTask();

            double getClock();

        private:
            double clock;
            WorkflowTask *task;
        };
    }
}

#endif //PEGASUS_PEGASUSSIMULATIONTIMESTAMPTYPES_H
