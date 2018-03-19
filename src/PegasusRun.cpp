/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include <iostream>
#include <vector>
#include <wrench-dev.h>

#include "DAGMan.h"

int main(int argc, char **argv) {

  //create and initialize the simulation
  wrench::Simulation simulation;
  simulation.init(&argc, argv);

  //check to make sure there are the right number of arguments
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <xml platform file> <dax file>" << std::endl;
    exit(1);
  }

  //create the platform file and dax file from command line args
  char *platform_file = argv[1];
  char *workflow_file = argv[2];
//  char *platform_file = "/Users/jamesoeth/CLionProjects/pegasus/twotasks.xml";
//  char *dax_file = "/Users/jamesoeth/CLionProjects/pegasus/onetask.dax";

  //loading the workflow from the dax file
  wrench::Workflow workflow;
  workflow.loadFromDAX(workflow_file);
  std::cout << "The workflow has " << workflow.getNumberOfTasks() << " tasks " << std::endl;
  std::cerr.flush();

  std::cerr << "Instantiating SimGrid platform..." << std::endl;
  simulation.instantiatePlatform(platform_file);

  std::vector<std::string> hostname_list = simulation.getHostnameList();

  std::string storage_host = hostname_list[(hostname_list.size() > 3) ? 2 : 1];

  std::cerr << "Instantiating a SimpleStorageService on " << storage_host << "..." << std::endl;

  wrench::StorageService *storage_service = simulation.add(
          new wrench::SimpleStorageService(storage_host, 10000000000000.0));

  std::string wms_host = hostname_list[0];

  std::string executor_host = hostname_list[(hostname_list.size() > 1) ? 1 : 0];
  std::vector<std::string> execution_hosts = {executor_host};

  // create the HTCondor services
  wrench::pegasus::HTCondorService *htcondor_service = new wrench::pegasus::HTCondorService(
          wms_host, "local", true, false, execution_hosts, storage_service);

  /* Add the cloud service to the simulation, catching a possible exception */
  try {
    simulation.add(htcondor_service);

  } catch (std::invalid_argument &e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::exit(1);
  }

  std::string file_registry_service_host = hostname_list[(hostname_list.size() > 2) ? 1 : 0];
  std::cerr << "Instantiating a FileRegistryService on " << file_registry_service_host << "..." << std::endl;
  wrench::FileRegistryService *file_registry_service = new wrench::FileRegistryService(file_registry_service_host);
  simulation.setFileRegistryService(file_registry_service);

  // create the DAGMan wms
  wrench::WMS *dagman = simulation.add(new wrench::pegasus::DAGMan(wms_host,
                                                                   {htcondor_service},
                                                                   {storage_service},
                                                                   file_registry_service));

  dagman->addWorkflow(&workflow);

  std::cerr << "Staging input files..." << std::endl;
  std::map<std::string, wrench::WorkflowFile *> input_files = workflow.getInputFiles();
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
