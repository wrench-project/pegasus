/**
 * Copyright (c) 2017-2018. The WRENCH Team.
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

#include "HTCondorService.h"

namespace wrench {
    namespace pegasus {

        class SimulationConfig {

        public:
            void loadProperties(wrench::Simulation *simulation, const std::string &filename);

            std::string getSubmitHostname();

            std::string getFileRegistryHostname();

            HTCondorService *getHTCondorService();

            std::set<StorageService*> getStorageServices();

        private:
            void instantiateMultihostMulticore(wrench::Simulation *simulation, std::vector<std::string> hosts);

            /**
             * @brief Get the value for a key from the JSON properties file
             *
             * @tparam T
             * @param keyName: property key that will be searched for
             * @param jsonData: JSON object to extract the value for the provided key
             *
             * @throw std::invalid_argument
             * @return The value for the provided key, if present
             */
            template<class T>
            T getPropertyValue(const std::string &keyName, const nlohmann::json &jsonData) {
              if (jsonData.find(keyName) == jsonData.end()) {
                throw std::invalid_argument("SimulationConfig::loadProperties(): Unable to find " + keyName);
              }
              return jsonData.at(keyName);
            }

            std::string submit_hostname;
            std::string file_registry_hostname;
            std::set<std::shared_ptr<ComputeService>> compute_services;
            std::set<StorageService *> storage_services;
            HTCondorService *htcondor_service;
        };

    }
}

#endif //PEGASUS_SIMULATIONCONFIG_H
