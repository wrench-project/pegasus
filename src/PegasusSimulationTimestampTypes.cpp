/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "PegasusSimulationTimestampTypes.h"

namespace wrench {
    namespace pegasus {

        /**
         * @brief
         *
         * @param task
         */
        SimulationTimestampJobSubmitted::SimulationTimestampJobSubmitted(WorkflowTask *task)
                : clock(S4U_Simulation::getClock()), task(task) {}

        /**
         * @brief
         *
         * @return
         */
        WorkflowTask *SimulationTimestampJobSubmitted::getTask() {
          return this->task;
        }

        /**
         * @brief
         *
         * @return
         */
        double SimulationTimestampJobSubmitted::getClock() {
          return this->clock;
        }

        /**
         * @brief
         *
         * @param task
         */
        SimulationTimestampJobScheduled::SimulationTimestampJobScheduled(WorkflowTask *task)
                : clock(S4U_Simulation::getClock()), task(task) {}

        /**
         * @brief
         *
         * @return
         */
        WorkflowTask *SimulationTimestampJobScheduled::getTask() {
          return this->task;
        }

        /**
         * @brief
         *
         * @return
         */
        double SimulationTimestampJobScheduled::getClock() {
          return this->clock;
        }

        /**
         * @brief
         *
         * @param task
         */
        SimulationTimestampJobCompletion::SimulationTimestampJobCompletion(WorkflowTask *task)
                : clock(S4U_Simulation::getClock()), task(task) {}

        /**
         * @brief
         *
         * @return
         */
        WorkflowTask *SimulationTimestampJobCompletion::getTask() {
          return this->task;
        }

        /**
         * @brief
         *
         * @return
         */
        double SimulationTimestampJobCompletion::getClock() {
          return this->clock;
        }
    }
}
