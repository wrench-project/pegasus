/**
 * Copyright (c) 2017-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef PEGASUS_SIMULATIONCONFIG_H
#define PEGASUS_SIMULATIONCONFIG_H

#include <nlohmann/json.hpp>
#include <wrench-dev.h>

namespace wrench {
    namespace pegasus {

        class SimulationConfig {
        public:
            void loadProperties(wrench::Simulation &simulation, const std::string &filename);

            std::string getSubmitHostname();

            std::string getFileRegistryHostname();

            std::shared_ptr<HTCondorComputeService> getHTCondorService();

            std::set<std::shared_ptr<StorageService>> getStorageServices();

            std::map<std::string, std::shared_ptr<StorageService>> getStorageServicesMap();

            std::vector<std::string> getExecutionHosts();

            std::string getEnergyScheme();

        private:
            void instantiateBareMetal(std::vector<std::string> hosts);

            void instantiateCloud(std::string service_host, std::vector<std::string> hosts);

            /**
             * @brief Get the value for a key from the JSON properties file
             *
             * @tparam T
             * @param keyName: property key that will be searched for
             * @param jsonData: JSON object to extract the value for the provided key
             * @param required: whether the property must exist
             *
             * @throw std::invalid_argument
             * @return The value for the provided key, if present
             */
            template<class T>
            T getPropertyValue(const std::string &keyName, const nlohmann::json &jsonData, const bool required = true) {
                if (jsonData.find(keyName) == jsonData.end()) {
                    if (required) {
                        throw std::invalid_argument("SimulationConfig::loadProperties(): Unable to find " + keyName);
                    } else {
                        return T();
                    }
                }
                return jsonData.at(keyName);
            }

            std::string submit_hostname;
            std::string file_registry_hostname;
            std::set<ComputeService *> compute_services;
            std::set<std::shared_ptr<StorageService>> storage_services;
            std::vector<std::string> execution_hosts;
            std::shared_ptr<HTCondorComputeService> htcondor_service;
            std::string energy_scheme;
        };

    }
}

#endif //PEGASUS_SIMULATIONCONFIG_H
