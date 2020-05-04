/**
 * Copyright (c) 2017-2020. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <iostream>
#include <vector>
#include <wrench-dev.h>
#include <wrench/tools/pegasus/PegasusWorkflowParser.h>

#include "DAGMan.h"
#include "PegasusSimulationTimestampTypes.h"
#include "SimulationConfig.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(PegasusRun, "Log category for PegasusRun");

int main(int argc, char **argv) {
    // create and initialize the simulation
    wrench::Simulation simulation;
    simulation.init(&argc, argv);

    // check to make sure there are the right number of arguments
    if (argc != 4) {
        std::cerr << "WRENCH Pegasus WMS Simulator" << std::endl;
        std::cerr << "Usage: " << argv[0]
                  << " <xml platform file> <JSON or XML workflow file> <JSON simulation config file>"
                  << std::endl;
        exit(1);
    }

    //create the platform file and dax file from command line args
    char *platform_file = argv[1];
    char *workflow_file = argv[2];
    char *properties_file = argv[3];

    // instantiating SimGrid platform
    WRENCH_INFO("Instantiating SimGrid platform from: %s", platform_file);
    simulation.instantiatePlatform(platform_file);

    // loading config file
    WRENCH_INFO("Loading simulation config from: %s", properties_file);
    wrench::pegasus::SimulationConfig config;
    config.loadProperties(simulation, properties_file);

    // loading the workflow from the JSON or XML file
    WRENCH_INFO("Loading workflow from: %s", workflow_file);
    std::istringstream ss(workflow_file);
    std::string token;
    std::vector<std::string> tokens;
    while (std::getline(ss, token, '.')) {
        tokens.push_back(token);
    }

    if (tokens.size() < 2) {
        std::cerr << "Invalid workflow file name " << workflow_file << " (should be *.xml or *.json)\n";
        exit(1);
    }
    wrench::Workflow *workflow;
    if (tokens[tokens.size() - 1] == "xml") {
        workflow = wrench::PegasusWorkflowParser::createWorkflowFromDAX(workflow_file, "1f");
    } else if (tokens[tokens.size() - 1] == "json") {
        workflow = wrench::PegasusWorkflowParser::createWorkflowFromJSON(workflow_file, "1f");
    } else {
        std::cerr << "Invalid workflow file name " << workflow_file << " (should be *.xml or *.json)\n";
        exit(1);
    }

    WRENCH_INFO("The workflow has %ld tasks", workflow->getNumberOfTasks());

    // create the HTCondor services
    std::shared_ptr<wrench::HTCondorComputeService> htcondor_service = config.getHTCondorService();

    // file registry service
    WRENCH_INFO("Instantiating a FileRegistryService on: %s", config.getFileRegistryHostname().c_str());
    std::shared_ptr<wrench::FileRegistryService> file_registry_service = simulation.add(
            new wrench::FileRegistryService(config.getFileRegistryHostname()));

    // create the DAGMan wms
    auto dagman = simulation.add(new wrench::pegasus::DAGMan(config.getSubmitHostname(),
                                                             {htcondor_service},
                                                             config.getStorageServices(),
                                                             file_registry_service,
                                                             config.getEnergyScheme()));
    dagman->addWorkflow(workflow);
    dagman->setExecutionHosts(config.getExecutionHosts());

    // stage input data
    WRENCH_INFO("Staging workflow input files to external Storage Service...");
    std::map<std::string, std::shared_ptr<wrench::StorageService>> storage_services = config.getStorageServicesMap();

    for (auto file : workflow->getInputFiles()) {
        for (auto storage_service : storage_services) {
            simulation.stageFile(file, storage_service.second);
        }
    }

    // simulation execution
    WRENCH_INFO("Launching the Simulation...");
    try {
        simulation.launch();
    } catch (std::runtime_error &e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 0;
    }WRENCH_INFO("Simulation done!");

    // statistics
    std::map<std::string, double> start_stats;
    std::map<std::string, double> completion_stats;
    std::map<std::string, double> scheduled_stats;
    std::map<std::string, double> duration_stats;
    std::map<std::string, unsigned long> level_stats;

    auto submit_trace = simulation.getOutput().getTrace<wrench::pegasus::SimulationTimestampJobSubmitted>();
    for (auto &task : submit_trace) {
        auto t = task->getContent();
        start_stats.insert(std::make_pair(t->getTask()->getID(), t->getClock()));
    }

    auto scheduled_trace = simulation.getOutput().getTrace<wrench::pegasus::SimulationTimestampJobScheduled>();
    for (auto &task : scheduled_trace) {
        auto t = task->getContent();
        scheduled_stats.insert(std::make_pair(t->getTask()->getID(), t->getClock()));
    }

    auto completion_trace = simulation.getOutput().getTrace<wrench::pegasus::SimulationTimestampJobCompletion>();
    for (auto &task : completion_trace) {
        auto t = task->getContent();
        duration_stats.insert(
                std::make_pair(t->getTask()->getID(), t->getTask()->getEndDate() - t->getTask()->getStartDate()));
        completion_stats.insert(std::make_pair(t->getTask()->getID(), t->getClock()));
        level_stats.insert(std::make_pair(t->getTask()->getID(), t->getTask()->getTopLevel()));
    }

    std::cerr << "=== WRENCH-Pegasus: Task Execution Summary" << std::endl;
    for (const auto &task : start_stats) {
        auto completion_time = (*completion_stats.find(task.first)).second;
        double duration = completion_time - (*scheduled_stats.find(task.first)).second;
        unsigned long level = (*level_stats.find(task.first)).second;

        std::string task_name = task.first.substr(0, task.first.find("_"));
        if (task_name == "clean") {
            task_name = "clean_up";
        } else if (task_name == "stage") {
            task_name = task.first.substr(0, task.first.find("_", task.first.find("_") + 1));
        } else if (task_name == "create") {
            task_name = "create_dir";
        }

        std::cerr << "wrench," <<
                  task.first << "," <<
                  task.second << "," <<
                  completion_time << "," <<
                  completion_time - task.second << "," <<
                  duration << "," <<
                  level << "," <<
                  task_name <<
                  std::endl;
    }

    if (not config.getEnergyScheme().empty()) {
        std::cerr << "=== WRENCH-Pegasus: Energy Profile Summary" << std::endl;
        auto power_trace = simulation.getOutput().getTrace<wrench::SimulationTimestampEnergyConsumption>();
        std::map<std::string, std::pair<double, unsigned long>> average_task_power;
        for (auto measurement : power_trace) {
            if (average_task_power.find(measurement->getContent()->getHostname()) == average_task_power.end()) {
                average_task_power.insert(std::make_pair(measurement->getContent()->getHostname(),
                                                         std::make_pair(measurement->getContent()->getConsumption(),
                                                                        1)));
            } else {
                average_task_power[measurement->getContent()->getHostname()].first += measurement->getContent()->getConsumption();
                average_task_power[measurement->getContent()->getHostname()].second++;
            }
        }

        for (auto it : average_task_power) {
            std::cerr << wrench::S4U_Simulation::getHostNumCores("taurus-worker.lyon.grid5000.fr") << "," <<
                      config.getEnergyScheme() << "," <<
                      it.first << "," <<
                      it.second.first / it.second.second << "," <<
                      "wrench-pegasus" <<
                      std::endl;
        }
    }

    return 0;
}
