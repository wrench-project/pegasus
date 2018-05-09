/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "HTCondorSchedd.h"


XBT_LOG_NEW_DEFAULT_CATEGORY(HTCondorSchedd, "Log category for HTCondor Scheduler Daemon");

namespace wrench {
    namespace pegasus {

        /**
         * @brief Schedule and run a set of ready tasks on available HTCondor resources
         *
         * @param compute_services: a set of compute services available to run jobs
         * @param tasks: a map of (ready) workflow tasks
         *
         * @throw std::runtime_error
         */

        int HTCondorSchedd::getJobNums(std::vector<unsigned long> num ){
            for(int i = 0; i < num.size(); i++){
                if(num[i] > 0){
                    return i;
                }
            }
            return 0;
        }

        int HTCondorSchedd::getNumAvailiableCores(std::vector<unsigned long> num ){
            int count = 0;
            for(int i = 0; i < num.size(); i++){
                count += num[i];
            }
            return count;
        }


        void HTCondorSchedd::scheduleTasks(const std::set<ComputeService *> &compute_services,
                                           const std::map<std::string, std::vector<WorkflowTask *>> &tasks) {

          WRENCH_INFO("There are %ld ready tasks to schedule", tasks.size());
            int computeNum = 0;
          std::set<ComputeService *> available_resources = compute_services;
            std::vector<WorkflowTask *> jobTasks;
            for (auto itc : tasks) {
                //vector to hold the workflow tasks to make the job
                std::set<ComputeService *>::iterator numCores = compute_services.begin();
                std::vector<unsigned long> num = (*numCores)->getNumIdleCores();
                int aComp = getJobNums(num);
                int count = num[aComp]-1;
                std::cout << "there are " << getNumAvailiableCores(num) << " availiable cores" << std::endl;
                if(getNumAvailiableCores(num) !=0){
                    for(auto it : itc.second) {
                        if (jobTasks.size() <= count) {
                            std::cout << "adding job to task vector" << std::endl;
                            jobTasks.push_back(it);
                        } else {
                            //send the job to be done!!
                            std::cout << "sending job to be done" << tasks.size() << std::endl;
                            WorkflowJob *job = (WorkflowJob *) this->getJobManager()->createStandardJob(jobTasks, {});
                            this->getJobManager()->submitJob(job, (*compute_services.begin()));
                            jobTasks.clear();
                            jobTasks.push_back(it);
                        }
                    }
                }
            }
            if(jobTasks.size() == tasks.size() and jobTasks.size() != 0){
                std::cout << jobTasks.size() << "  jobs task " << tasks.size() << "  tasks size " << std::endl;
                std::cout << "sending jobbb to be done" << jobTasks.size() << std::endl;
                WorkflowJob *job = (WorkflowJob *) this->getJobManager()->createStandardJob(jobTasks, {});
                this->getJobManager()->submitJob(job, (*compute_services.begin()));
            }
//
//                for(unsigned int i = 0; i < num.size(); i++){
//                for(unsigned int j = 0; j < num[i]; j++){
//                    if(it != tasks.end()){
//                        jobTasks.push_back (itc->second);
//                    }
//
//                    WorkflowJob *job = (WorkflowJob *) this->getJobManager()->createStandardJob(itc.second, {});
//
//                }
            }

//          for (auto itc : tasks) {
//            // TODO add support to pilot jobs
//
//
//              WorkflowJob *job = (WorkflowJob *) this->getJobManager()->createStandardJob(itc.second, {});
//
//
//
//              for (auto compute_service : available_resources) {
//              unsigned long sum_num_idle_cores = 0;
//
//              try {
//                std::vector<unsigned long> num_idle_cores = compute_service->getNumIdleCores();
//                sum_num_idle_cores = (unsigned long) std::accumulate(num_idle_cores.begin(), num_idle_cores.end(), 0);
//              } catch (WorkflowExecutionException &e) {
//                // The service has some problem, forget it
//                throw std::runtime_error("Unable to get the number of idle cores.");
//              }
//
//              unsigned long mim_num_cores = ((StandardJob *) (job))->getMinimumRequiredNumCores();
//              if (sum_num_idle_cores >= mim_num_cores) {
//                this->getJobManager()->submitJob(job, compute_service);
//                available_resources.erase(compute_service);
//                break;
//              }
//            }
//          }
//          WRENCH_INFO("Done with scheduling tasks as standard jobs");
//        }
    }
}
