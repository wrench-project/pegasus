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
#include "SimulationConfig.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(PegasusRun, "Log category for PegasusRun");

int main(int argc, char **argv) {

  // create and initialize the simulation
  wrench::Simulation simulation;
  simulation.init(&argc, argv);

  // check to make sure there are the right number of arguments
  if (argc != 4) {
    std::cerr << "WRENCH Pegasus WMS Simulator" << std::endl;
    std::cerr << "Usage: " << argv[0] << " <xml platform file> <JSON workflow file> <JSON simulation config file>"
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

  // loading the workflow from the JSON file
  WRENCH_INFO("Loading workflow from: %s", workflow_file);
  wrench::Workflow workflow;
  workflow.loadFromJSON(workflow_file, "1f");
  WRENCH_INFO("The workflow has %ld tasks", workflow.getNumberOfTasks());

  // create the HTCondor services
  wrench::pegasus::HTCondorService *htcondor_service = config.getHTCondorService();

  // file registry service
  WRENCH_INFO("Instantiating a FileRegistryService on: %s", config.getFileRegistryHostname().c_str());
  wrench::FileRegistryService *file_registry_service = simulation.add(
          new wrench::FileRegistryService(config.getFileRegistryHostname()));

  // create the DAGMan wms
  wrench::WMS *dagman = simulation.add(new wrench::pegasus::DAGMan(config.getSubmitHostname(),
                                                                   {htcondor_service},
                                                                   config.getStorageServices(),
                                                                   file_registry_service));
  dagman->addWorkflow(&workflow);

  WRENCH_INFO("Staging workflow input files to external Storage Service...");
  std::map<std::string, wrench::WorkflowFile *> input_files = workflow.getInputFiles();

  auto storage_service_it = config.getStorageServices().begin();

  try {
    // TODO: improve stage in data
    simulation.stageFiles(input_files, *storage_service_it);
  } catch (std::runtime_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 0;
  }

  // simulation execution
  WRENCH_INFO("Launching the Simulation...");
  try {
    simulation.launch();
  } catch (std::runtime_error &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
    return 0;
  }
  WRENCH_INFO("Simulation done!");

  // statistics
  std::vector<wrench::SimulationTimestamp<wrench::SimulationTimestampTaskCompletion> *> trace;
  trace = simulation.output.getTrace<wrench::SimulationTimestampTaskCompletion>();
  WRENCH_INFO("Number of entries in TaskCompletion trace: %ld", trace.size());

  double lastTime = 0;
  double totalTime = 0;
  for (auto &task : trace) {
    std::cerr << "Task in trace entry: " << task->getContent()->getTask()->getId() << " with time:  "
              << task->getContent()->getTask()->getEndDate() - task->getContent()->getTask()->getStartDate()
              << std::endl;
    lastTime = std::max(lastTime, task->getContent()->getTask()->getEndDate());
    totalTime += task->getContent()->getTask()->getEndDate();
  }

  std::cerr << "Total Time: " << lastTime << std::endl;
  std::cerr << "Total Time: " << totalTime << std::endl;
  return 0;

}
