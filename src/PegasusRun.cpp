/**
 * Copyright (c) 2017. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <string>
#include <iostream>
#include <vector>
#include "wrench-dev.h"
#include "DagMan.h"
#include "HTCondor.h"


using namespace std;




int main(int argc, char **argv) {

    //create and initialize the simulation
    wrench::Simulation simulation;
    simulation.init(&argc, argv);
    //check to make sure there are the right number of arguments
//       if (argc != 3) {
//           std::cerr << "Usage: " << argv[0] << " <xml platform file> <dax file>" << std::endl;
//           exit(1);
//       }

    //create the platform file and dax file from command line args
    char *platform_file = "/Users/jamesoeth/CLionProjects/pegasus/twotasks.xml";
    char *dax_file = "/Users/jamesoeth/CLionProjects/pegasus/onetask.dax";

    //making the actual workflow
    std::cerr << "Creating a bogus workflow..." << std::endl;
    wrench::Workflow workflow;

    //loading the workflow from the dax file
    std::cout << "creating the dax file" << std::endl;
    workflow.loadFromDAX(dax_file);
    std::cout << "The workflow has " << workflow.getNumberOfTasks() << " tasks " << std::endl;
    std::cerr.flush();


    std::cerr << "Instantiating SimGrid platform..." << std::endl;
    simulation.instantiatePlatform(platform_file);

    std::vector<std::string> hostname_list = simulation.getHostnameList();

    std::string storage_host = hostname_list[(hostname_list.size() > 3) ? 2 : 1];

    std::cerr << "Instantiating a SimpleStorageService on " << storage_host << "..." << std::endl;

    wrench::StorageService *storage_service = simulation.add(std::unique_ptr<wrench::SimpleStorageService>(
            new wrench::SimpleStorageService(storage_host, 10000000000000.0)));


    std::string wms_host = hostname_list[0];


    std::string executor_host = hostname_list[(hostname_list.size() > 1) ? 1 : 0];
    std::vector<std::string> execution_hosts = {executor_host};


    //TODO: make the batch compute service
    //here is the htcondor service
//    HTCondorService *HTCondor_Service = new HTCondorService(
//            wms_host, true, true, storage_service,
//            {{HTCondorServiceProperty::STOP_DAEMON_MESSAGE_PAYLOAD, "666"}});
//
    //create a cloud service
    wrench::CloudService *cloud_service = new wrench::CloudService(
            wms_host, true, true, execution_hosts, storage_service,
            {{wrench::CloudServiceProperty::STOP_DAEMON_MESSAGE_PAYLOAD, "1024"}});

    try {

        std::cerr << "Instantiating a htcondor Job executor on " << executor_host << "..." << std::endl;
        simulation.add(std::unique_ptr<wrench::ComputeService>(cloud_service));

    } catch (std::invalid_argument &e) {

        std::cerr << "Error: " << e.what() << std::endl;
        std::exit(1);

    }

    std::cerr << "Instantiating a WMS on " << wms_host << "..." << std::endl;
    std::vector<wrench::ComputeService *> computeService;
    computeService.push_back(cloud_service);
    // WMS Configuration
//    wrench::WMS *wms = simulation.add(
//            std::unique_ptr<wrench::WMS>(
//                    new DagMan(&workflow,
//                               std::unique_ptr<wrench::Scheduler>(
//                                       //new HTCondor(computeService, execution_hosts,
//                                         //           &simulation)),
//                               wms_host)));

    //  wms->setPilotJobScheduler(std::unique_ptr<wrench::PilotJobScheduler>(new wrench::CriticalPathScheduler()));

    //  wms->addStaticOptimization(std::unique_ptr<wrench::StaticOptimization>(new wrench::SimplePipelineClustering()));
    //  wms->addDynamicOptimization(std::unique_ptr<wrench::DynamicOptimization>(new wrench::FailureDynamicClustering()));

    std::string file_registry_service_host = hostname_list[(hostname_list.size() > 2) ? 1 : 0];

    std::cerr << "Instantiating a FileRegistryService on " << file_registry_service_host << "..." << std::endl;
    std::unique_ptr<wrench::FileRegistryService> file_registry_service(
            new wrench::FileRegistryService(file_registry_service_host));
    simulation.setFileRegistryService(std::move(file_registry_service));

    std::cerr << "Staging input files..." << std::endl;
    std::set<wrench::WorkflowFile *> input_files = workflow.getInputFiles();
    try {
        simulation.stageFiles(input_files, storage_service);
    } catch (std::runtime_error &e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 0;
    }

    std::cerr << "Launching the Simulation..." << std::endl;
    try {
        simulation.launch();
    } catch (std::runtime_error &e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 0;
    }
    std::cerr << "Simulation done!" << std::endl;

    std::vector<wrench::SimulationTimestamp<wrench::SimulationTimestampTaskCompletion> *> trace;
    trace = simulation.output.getTrace<wrench::SimulationTimestampTaskCompletion>();
    std::cerr << "Number of entries in TaskCompletion trace: " << trace.size() << std::endl;
    std::cerr << "Task in first trace entry: " << trace[0]->getContent()->getTask()->getId() << std::endl;

    return 0;

}
