/**
 * Copyright (c) 2017-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "PowerMeter.h"
#include "wrench/simgrid_S4U_util/S4U_Simulation.h"

#define EPSILON 0.0001

XBT_LOG_NEW_DEFAULT_CATEGORY(power_meter, "Log category for Power Meter");

namespace wrench {
    namespace pegasus {

        /**
         * @brief Constructor
         *
         * @param wms: the WMS that uses this power meter
         * @param hostnames: the list of metered hosts, as hostnames
         * @param measurement_period: the measurement period
         * @param pairwise: whether cores in socket are enabled in pairwise manner
         */
        PowerMeter::PowerMeter(WMS *wms, const std::vector<std::string> &hostnames, double measurement_period,
                               bool pairwise) :
                Service(wms->hostname, "power_meter", "power_meter"), pairwise(pairwise) {
            if (hostnames.empty()) {
                throw std::invalid_argument("PowerMeter::PowerMeter(): no host to meter!");
            }
            if (measurement_period < 1) {
                throw std::invalid_argument("PowerMeter::PowerMeter(): measurement period must be at least 1 second");
            }

            this->wms = wms;

            for (auto const &h : hostnames) {
                if (not S4U_Simulation::hostExists(h)) {
                    throw std::invalid_argument("PowerMeter::PowerMeter(): unknown host " + h);
                }
                this->measurement_periods[h] = measurement_period;
                this->time_to_next_measurement[h] = 0.0;  // We begin by taking a measurement
            }
        }

        /**
         * @brief Compare the start time between two workflow tasks
         *
         * @param lhs: pointer to a workflow task
         * @param rhs: pointer to a workflow task
         *
         * @return whether the start time of the left-hand-side workflow tasks is earlier
         */
        bool PowerMeter::TaskStartTimeComparator::operator()(WorkflowTask *&lhs, WorkflowTask *&rhs) {
            return lhs->getStartDate() < rhs->getStartDate();
        }

        /**
         * @brief Main method of the daemon that implements the PowerMeter
         * @return 0 on success
         */
        int PowerMeter::main() {
            TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_YELLOW);

            WRENCH_INFO("New Power Meter starting (%s)", this->mailbox_name.c_str());

            bool life = true;

            /** Main loop **/
            while (life) {
                S4U_Simulation::computeZeroFlop();

                // Find the minimum of the times to next measurements
                auto min_el = std::min_element(
                        this->time_to_next_measurement.begin(),
                        this->time_to_next_measurement.end(),
                        [](decltype(this->time_to_next_measurement)::value_type &lhs,
                           decltype(this->time_to_next_measurement)::value_type &rhs) {
                            return lhs.second < rhs.second;
                        });

                double time_to_next_measurement = min_el->second;
                double before = Simulation::getCurrentSimulatedDate();

                if (time_to_next_measurement > 0) {
                    life = this->processNextMessage(time_to_next_measurement);
                }
                if (not life) {
                    break;
                }

                // Update time-to-next-measurement for all hosts
                for (auto &h : this->time_to_next_measurement) {
                    h.second = std::max<double>(0, h.second - (Simulation::getCurrentSimulatedDate() - before));
                }

                // TODO: add comparator by start time
                std::map<std::string, std::set<WorkflowTask *>> tasks_per_host;

                // Take measurements
                for (auto &h : this->time_to_next_measurement) {
                    if (h.second < EPSILON) {

                        for (auto task : this->wms->getWorkflow()->getTasks()) {

                            // only process running tasks
                            if (task->getStartDate() != -1 && task->getEndDate() == -1) {
                                if (tasks_per_host.find(task->getExecutionHost()) == tasks_per_host.end()) {
                                    std::set<WorkflowTask *> tasks_set;
                                    tasks_set.insert(task);
                                    tasks_per_host.insert(std::make_pair(task->getExecutionHost(), tasks_set));
                                } else {
                                    tasks_per_host[task->getExecutionHost()].insert(task);
                                }
                            }
                        }

                        this->time_to_next_measurement[h.first] = this->measurement_periods[h.first];
                    }
                }

                // compute power consumption
                for (auto &key_value : tasks_per_host) {
                    this->computePowerMeasurements(key_value.first, key_value.second, true);
                }
            }

            WRENCH_INFO("Energy Meter Manager terminating");

            return 0;
        }

        /**
         * @brief Obtain the current power consumption of a host and will add SimulationTimestampEnergyConsumption to
         *          simulation output if can_record is set to true
         *
         * @param hostname: the host name
         * @param tasks: list of WorkflowTask running on the host
         * @param record_as_time_stamp: bool signaling whether or not to record a SimulationTimestampEnergyConsumption object
         *
         * @return current energy consumption in Watts
         * @throw std::invalid_argument
         */
        double PowerMeter::computePowerMeasurements(const std::string &hostname,
                                                    std::set<WorkflowTask *> tasks,
                                                    bool record_as_time_stamp) {
            if (hostname.empty()) {
                throw std::invalid_argument("Simulation::getEnergyConsumed() requires a valid hostname");
            }

            double consumption = S4U_Simulation::getMinPowerConsumption(hostname);
            int task_index = 0;
            std::string task_name;
            double task_factor = 1;

            for (auto task : tasks) {
                double task_consumption = 0;
                task_name = task->getID();

                // power related to cpu usage
                // dynamic power per socket
                double dynamic_power =
                        (S4U_Simulation::getMaxPowerConsumption(hostname) -
                         S4U_Simulation::getMinPowerConsumption(hostname)) *
                        (task->getAverageCPU() / 100) / 2;

                if (this->pairwise && task_index < 2) {
                    task_consumption = dynamic_power / 6;

                } else if (this->pairwise && task_index >= 2) {
                    task_consumption = task_factor * dynamic_power / 6;
                    task_factor *= 0.88;

                } else if (not this->pairwise && std::fmod(task_index, 6) == 0) {
                    task_consumption = dynamic_power / 6;
                    task_factor = 1;

                } else {
                    task_consumption = task_factor * dynamic_power / 6;
                    task_factor *= 0.9;
                }

                // power related to IO usage
                task_consumption += task_consumption * (this->pairwise ? 0.486 : 0.213);

                // IOWait factor
//            task_consumption *= 1.31;

                consumption += task_consumption;
                task_index++;
            }

            if (record_as_time_stamp) {
                if (tasks.size() == S4U_Simulation::getHostNumCores(hostname)) {
                    this->simulation->getOutput().addTimestampEnergyConsumption(
                            task_name.substr(0, task_name.find("_")), consumption);
                }
            }

            return consumption;
        }

        /**
         * @brief Process the next message
         * @return true if the daemon should continue, false otherwise
         *
         * @throw std::runtime_error
         */
        bool PowerMeter::processNextMessage(double timeout) {
            std::shared_ptr<SimulationMessage> message = nullptr;

            try {
                message = S4U_Mailbox::getMessage(this->mailbox_name, timeout);
            } catch (std::shared_ptr<NetworkError> &cause) {
                return true;
            }

            if (message == nullptr) { WRENCH_INFO("Got a NULL message... Likely this means we're all done. Aborting!");
                return false;
            }

            WRENCH_INFO("Power Meter got a %s message", message->getName().c_str());

            if (auto msg = dynamic_cast<ServiceStopDaemonMessage *>(message.get())) {
                // There shouldn't be any need to clean any state up
                return false;

            } else {
                throw std::runtime_error(
                        "PowerMeter::waitForNextMessage(): Unexpected [" + message->getName() + "] message");
            }
        }

        /**
         * @brief Kill the power meter (brutally terminate the daemon)
         */
        void PowerMeter::kill() {
            this->killActor();
        }

        /**
         * @brief Stop the power meter
         *
         * @throw WorkflowExecutionException
         * @throw std::runtime_error
         */
        void PowerMeter::stop() {
            try {
                S4U_Mailbox::putMessage(this->mailbox_name, new ServiceStopDaemonMessage("", 0.0));
            } catch (std::shared_ptr<NetworkError> &cause) {
                throw WorkflowExecutionException(cause);
            }
        }
    }
}
