/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef PEGASUS_HTCONDORSERVICEMESSAGE_H
#define PEGASUS_HTCONDORSERVICEMESSAGE_H

#include "wrench-dev.h"
/***********************/
/** \cond INTERNAL     */
/***********************/

/**
 * @brief Top-level CloudServiceMessage class
 */
class HTCondorServiceMessage : public wrench::ComputeServiceMessage {
protected:
    HTCondorServiceMessage(const std::string &name, double payload);
};

/**
 * @brief CloudServiceGetExecutionHostsRequestMessage class
 */
class HTCondorServiceGetExecutionHostsRequestMessage : public HTCondorServiceMessage {
public:
    HTCondorServiceGetExecutionHostsRequestMessage(const std::string &answer_mailbox, double payload);

    std::string answer_mailbox;
};

/**
 * @brief CloudServiceGetExecutionHostsAnswerMessage class
 */
class HTCondorServiceGetExecutionHostsAnswerMessage : public HTCondorServiceMessage {
public:
    HTCondorServiceGetExecutionHostsAnswerMessage(std::vector<std::string> &execution_hosts, double payload);

    std::vector<std::string> execution_hosts;
};

/**
 * @brief CloudServiceCreateVMRequestMessage class
 */
class HTCondorServiceCreateVMRequestMessage : public HTCondorServiceMessage {
public:
    HTCondorServiceCreateVMRequestMessage(const std::string &answer_mailbox,
                                       const std::string &pm_hostname,
                                       const std::string &vm_hostname,
                                       int num_cores,
                                       bool supports_standard_jobs,
                                       bool supports_pilot_jobs,
                                       std::map<std::string, std::string> &plist,
                                       double payload);

    std::string pm_hostname;
    std::string vm_hostname;
    int num_cores;
    bool supports_standard_jobs;
    bool supports_pilot_jobs;
    std::map<std::string, std::string> plist;
    std::string answer_mailbox;
};

/**
 * @brief CloudServiceCreateVMAnswerMessage class
 */
class HTCondorServiceCreateVMAnswerMessage : public HTCondorServiceMessage {
public:
    HTCondorServiceCreateVMAnswerMessage(bool success, double payload);

    bool success;
};

/***********************/
/** \endcond           */
/***********************/


#endif //PEGASUS_HTCONDORSERVICEMESSAGE_H
