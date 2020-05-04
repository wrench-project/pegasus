/**
 * Copyright (c) 2017-2020. The WRENCH Team.
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
         *        HTCondorComputeService object.
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
            this->energy_scheme = getPropertyValue<std::string>("energy_scheme", json_data, false);

            // storage resources
            std::vector<nlohmann::json> storage_resources = json_data.at("storage_hosts");
            for (auto &storage : storage_resources) {
                std::string storage_host = getPropertyValue<std::string>("hostname", storage);WRENCH_INFO(
                        "Instantiating a SimpleStorageService on: %s", storage_host.c_str());
                auto storage_service = simulation.add(
//                    new SimpleStorageService(storage_host, {"/"}, getPropertyValue<double>("capacity", storage)));
                        new SimpleStorageService(storage_host, {"/"}));
                storage_service->setNetworkTimeoutValue(30);
                this->storage_services.insert(storage_service);
            }

            // compute resources
            std::vector<nlohmann::json> compute_resources = json_data.at("compute_services");
            for (auto &resource : compute_resources) {
                std::string type = getPropertyValue<std::string>("type", resource);

                if (type == "bare-metal" || type == "multicore") {
                    instantiateBareMetal(resource.at("compute_hosts"));
                } else if (type == "cloud") {
                    instantiateCloud(resource.at("service_host"), resource.at("compute_hosts"));
                } else if (type == "batch") {
                    throw std::invalid_argument(
                            "SimulationConfig::loadProperties(): Batch support not yet implemented");
                } else {
                    throw std::invalid_argument("SimulationConfig::loadProperties(): Invalid compute service type");
                }
            }

            // build the HTCondorComputeService
            this->htcondor_service = simulation.add(
                    new HTCondorComputeService(this->submit_hostname, "local", std::move(this->compute_services),
                                               {{ComputeServiceProperty::SUPPORTS_PILOT_JOBS, "false"}}));

            // creating local storage service
            auto local_storage_service = simulation.add(
                    new SimpleStorageService(this->submit_hostname, {"/"}));
            local_storage_service->setNetworkTimeoutValue(30);
            this->htcondor_service->setLocalStorageService(local_storage_service);
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
         * @brief Get a pointer to the HTCondorComputeService
         * @return A pointer to the HTCondorComputeService
         */
        std::shared_ptr<HTCondorComputeService> SimulationConfig::getHTCondorService() {
            return this->htcondor_service;
        }

        /**
         * @brief Get a set of pointers to wrench::StorageService available for the simulation
         * @return A set of storage services
         */
        std::set<std::shared_ptr<StorageService>> SimulationConfig::getStorageServices() {
            return this->storage_services;
        }

        /**
         * @brief Get a map of pointers to wrench::StorageService indexed by hostname
         * @return  A map of storage services
         */
        std::map<std::string, std::shared_ptr<StorageService>> SimulationConfig::getStorageServicesMap() {
            std::map<std::string, std::shared_ptr<StorageService>> map;
            for (auto storage_service : this->storage_services) {
                map.insert(std::make_pair(storage_service->getHostname(), storage_service));
            }
            return map;
        };

        /**
         * @brief Get a vector of execution hosts that can be used to compute tasks
         * @return A vector of execution hosts
         */
        std::vector<std::string> SimulationConfig::getExecutionHosts() {
            return this->execution_hosts;
        }

        /**
         * @brief Get the energy scheme (if provided)
         * @return the energy scheme (if provided) or blank string
         */
        std::string SimulationConfig::getEnergyScheme() {
            return this->energy_scheme;
        }

        /**
         * @brief Instantiate wrench::MultihostMulticoreComputeService
         *
         * @param hosts: vector of hosts to be instantiated
         */
        void SimulationConfig::instantiateBareMetal(std::vector<std::string> hosts) {
            std::map<std::string, double> messagepayload_properties_list = {
                    {BareMetalComputeServiceMessagePayload::SUBMIT_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD, 122880000},
                    {BareMetalComputeServiceMessagePayload::SUBMIT_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD,  1024},
                    {BareMetalComputeServiceMessagePayload::STANDARD_JOB_DONE_MESSAGE_PAYLOAD,           512000000},
            };

            for (auto &hostname : hosts) {
                this->execution_hosts.push_back(hostname);
                std::map<std::string, std::tuple<unsigned long, double>> compute_resources;
                compute_resources.insert(std::make_pair(hostname,
                                                        std::make_tuple(
                                                                wrench::Simulation::getHostNumCores(hostname),
                                                                wrench::Simulation::getHostMemoryCapacity(hostname))));
                this->compute_services.insert(
                        new BareMetalComputeService(hostname, compute_resources, {"/"}, {},
                                                    messagepayload_properties_list));
            }
        }

        /**
         * @brief Instantiate wrench::MultihostMulticoreComputeService
         *
         * @param service_host: name of the host to run the cloud service
         * @param hosts: vector of hosts to be instantiated
         */
        void SimulationConfig::instantiateCloud(std::string service_host, std::vector<std::string> hosts) {
            std::map<std::string, double> messagepayload_properties_list = {
                    {ComputeServiceMessagePayload::SUBMIT_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD, 122880000},
                    {ComputeServiceMessagePayload::SUBMIT_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD,  1024},
                    {ComputeServiceMessagePayload::STANDARD_JOB_DONE_MESSAGE_PAYLOAD,           512000000},
            };

            this->execution_hosts.insert(this->execution_hosts.end(), hosts.begin(), hosts.end());
            auto cloud_service = new VirtualizedClusterComputeService(service_host, hosts, {"/cloud"}, {},
                                                                      messagepayload_properties_list);

            this->compute_services.insert(cloud_service);
        }
    };
}
