//
// Created by james oeth on 12/29/17.
//

#include "HTCondorServiceMessage.h"
#include "wrench-dev.h"

/**
     * @brief Constructor
     *
     * @param name: the message name
     * @param payload: the message size in bytes
     */
HTCondorServiceMessage::HTCondorServiceMessage(const std::string &name, double payload) :
        wrench::ComputeServiceMessage("HTCondorServiceMessage::" + name, payload) {
}

/**
 * @brief Constructor
 *
 * @param answer_mailbox: the mailbox to which to send the answer
 * @param payload: the message size in bytes
 *
 * @throw std::invalid_argument
 */
HTCondorServiceGetExecutionHostsRequestMessage::HTCondorServiceGetExecutionHostsRequestMessage(
        const std::string &answer_mailbox, double payload) : HTCondorServiceMessage("GET_EXECUTION_HOSTS_REQUEST",
                                                                                 payload) {

    if (answer_mailbox.empty()) {
        throw std::invalid_argument(
                "HTCondorServiceGetExecutionHostsRequestMessage::HTCondorServiceGetExecutionHostsRequestMessage(): "
                        "Invalid arguments");
    }
    this->answer_mailbox = answer_mailbox;
}

/**
 * @brief Constructor
 *
 * @param execution_hosts: the hosts available for running virtual machines
 * @param payload: the message size in bytes
 */
HTCondorServiceGetExecutionHostsAnswerMessage::HTCondorServiceGetExecutionHostsAnswerMessage(
        std::vector<std::string> &execution_hosts, double payload) : HTCondorServiceMessage(
        "GET_EXECUTION_HOSTS_ANSWER", payload), execution_hosts(execution_hosts) {}

/**
 * @brief Constructor
 *
 * @param answer_mailbox: the mailbox to which to send the answer
 * @param pm_hostname: the name of the physical machine host
 * @param vm_hostname: the name of the new VM host
 * @param num_cores: the number of cores the service can use (0 means "use as many as there are cores on the host")
 * @param supports_standard_jobs: true if the compute service should support standard jobs
 * @param supports_pilot_jobs: true if the compute service should support pilot jobs
 * @param plist: a property list ({} means "use all defaults")
 * @param payload: the message size in bytes
 *
 * @throw std::invalid_argument
 */
HTCondorServiceCreateVMRequestMessage::HTCondorServiceCreateVMRequestMessage(const std::string &answer_mailbox,
                                                                       const std::string &pm_hostname,
                                                                       const std::string &vm_hostname,
                                                                       int num_cores,
                                                                       bool supports_standard_jobs,
                                                                       bool supports_pilot_jobs,
                                                                       std::map<std::string, std::string> &plist,
                                                                       double payload) :
        HTCondorServiceMessage("CREATE_VM_REQUEST", payload), num_cores(num_cores),
        supports_standard_jobs(supports_standard_jobs), supports_pilot_jobs(supports_pilot_jobs), plist(plist) {

    if (answer_mailbox.empty() || pm_hostname.empty() || vm_hostname.empty() || (num_cores < 0)) {
        throw std::invalid_argument(
                "HTCondorServiceCreateVMRequestMessage::HTCondorServiceCreateVMRequestMessage(): Invalid arguments");
    }
    this->answer_mailbox = answer_mailbox;
    this->pm_hostname = pm_hostname;
    this->vm_hostname = vm_hostname;
}

/**
 * @brief Constructor
 *
 * @param vm_hostname: the new VM hostname
 * @param payload: the message size in bytes
 */
HTCondorServiceCreateVMAnswerMessage::HTCondorServiceCreateVMAnswerMessage(bool success, double payload) :
        HTCondorServiceMessage("CREATE_VM_ANSWER", payload), success(success) {}