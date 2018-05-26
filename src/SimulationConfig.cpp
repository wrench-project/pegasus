/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <fstream>

#include "SimulationConfig.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(SimulationConfig, "Log category for SimulationConfig");

namespace wrench {
    namespace pegasus {

        /**
         * @brief Load the simulation properties for configuring the simulation. It also creates and configures the
         *        HTCondorService object.
         *
         * @param simulation: pointer to simulation object
         * @param filename: File path for the properties file
         *
         * @throw std::invalid_argument
         */
        void SimulationConfig::loadProperties(wrench::Simulation &simulation, const std::string &filename) {

          std::ifstream file;
          nlohmann::json json_data;

          // handle the exceptions of opening the json file
          file.exceptions(std::ifstream::failbit | std::ifstream::badbit);
          try {
            file.open(filename);
            file >> json_data;
          } catch (const std::ifstream::failure &e) {
            throw std::invalid_argument("SimulationConfig::loadProperties(): Invalid JSON file");
          }

          this->submit_hostname = getPropertyValue<std::string>("submit_host", json_data);
          this->file_registry_hostname = getPropertyValue<std::string>("file_registry_host", json_data);

          // storage resources
          std::vector<nlohmann::json> storage_resources = json_data.at("storage_hosts");
          for (auto &storage : storage_resources) {
            std::string storage_host = getPropertyValue<std::string>("hostname", storage);
            WRENCH_INFO("Instantiating a SimpleStorageService on: %s", storage_host.c_str());
            this->storage_services.insert(simulation.add(
                    new SimpleStorageService(storage_host, getPropertyValue<double>("capacity", storage))));
          }

          // compute resources
          std::vector<nlohmann::json> compute_resources = json_data.at("compute_services");
          for (auto &resource : compute_resources) {
            std::string type = getPropertyValue<std::string>("type", resource);

            if (type == "multicore") {
              instantiateMultihostMulticore(resource.at("compute_hosts"));
            } else if (type == "cloud") {
              // TODO: to be implemented
            } else if (type == "batch") {
              // TODO: to be implemented
            } else {
              throw std::invalid_argument("SimulationConfig::loadProperties(): Invalid compute service type");
            }
          }

          // build the HTCondorService
          this->htcondor_service = (HTCondorService *) simulation.add(
                  new HTCondorService(this->submit_hostname, "local", true, false,
                                      std::move(this->compute_services)));

          // creating local storage service
          this->htcondor_service->setLocalStorageService(
                  simulation.add(new SimpleStorageService(this->submit_hostname, 10000000000.0)));
        }

        /**
         * @brief Get the hostname for the submit host
         * @return The hostname for the submit host
         */
        std::string SimulationConfig::getSubmitHostname() {
          return this->submit_hostname;
        }

        /**
         * @brief Get the hostname for the file registry
         * @return The hostname for the file registry
         */
        std::string SimulationConfig::getFileRegistryHostname() {
          return this->file_registry_hostname;
        }

        /**
         * @brief Get a pointer to the HTCondorService
         * @return A pointer to the HTCondorService
         */
        HTCondorService *SimulationConfig::getHTCondorService() {
          return this->htcondor_service;
        }

        /**
         * @brief Get a set of pointers to wrench::StorageService available for the simulation
         * @return A set of storage services
         */
        std::set<StorageService *> SimulationConfig::getStorageServices() {
          return this->storage_services;
        }

        /**
         * @brief Get a map of pointers to wrench::StorageService indexed by hostname
         * @return  A map of storage services
         */
        std::map<std::string, StorageService *> SimulationConfig::getStorageServicesMap() {
          std::map<std::string, StorageService *> map;
          for (auto storage_service : this->storage_services) {
            map.insert(std::make_pair(storage_service->getHostname(), storage_service));
          }
          return map;
        };

        /**
         * @brief Instantiate wrench::MultihostMulticoreComputeService
         *
         * @param hosts: vector of hosts to be instantiated
         */
        void SimulationConfig::instantiateMultihostMulticore(std::vector<std::string> hosts) {

          // TODO: setup map of properties for simulation calibration
          std::map<std::string, std::string> properties_list = {
                  {MultihostMulticoreComputeServiceProperty::SUBMIT_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD, "122880000"},
                  {MultihostMulticoreComputeServiceProperty::SUBMIT_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD,  "1024"},
                  {MultihostMulticoreComputeServiceProperty::STANDARD_JOB_DONE_MESSAGE_PAYLOAD,           "512000000"},
          };

          for (auto &hostname : hosts) {
            this->compute_services.insert(std::shared_ptr<ComputeService>(
                    new MultihostMulticoreComputeService(
                            hostname, true, false,
                            {std::make_tuple(
                                    hostname,
                                    wrench::Simulation::getHostNumCores(hostname),
                                    wrench::Simulation::getHostMemoryCapacity(hostname))},
                            properties_list, 1000000000.0)));
          }
        }
    };
}
