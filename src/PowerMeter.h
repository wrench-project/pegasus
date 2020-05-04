/**
 * Copyright (c) 2017-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef PEGASUS_POWERMETER_H
#define PEGASUS_POWERMETER_H

#include <wrench-dev.h>

namespace wrench {
    namespace pegasus {

        class DAGMan;

        class PowerMeter : public Service {
        public:
            PowerMeter(WMS *wms, const std::vector<std::string> &hostnames, double period, bool pairwise = false);

            void kill();

            void stop() override;

        protected:
            friend class DAGMan;

        private:
            int main() override;

            struct TaskStartTimeComparator {
                bool operator()(WorkflowTask *&lhs, WorkflowTask *&rhs);
            };

            double computePowerMeasurements(const std::string &hostname,
                                            std::set<WorkflowTask *> tasks,
                                            bool record_as_time_stamp);

            bool processNextMessage(double timeout);

            // Relevant WMS
            WMS *wms;
            bool pairwise;
            std::map<std::string, double> measurement_periods;
            std::map<std::string, double> time_to_next_measurement;
        };
    }
}

#endif //PEGASUS_POWERMETER_H
