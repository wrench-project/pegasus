
//manages resource pool and receives jobs from dagman

#ifndef PROJECT_HTCONDOR_H
#define PROJECT_HTCONDOR_H

#include "wrench-dev.h"
#include "HTCondorServiceProperty.h"
/**
 * @brief A multicore Scheduler for schedd
 *
 */

class HTCondor : public wrench::ComputeService {
private:

    std::map<std::string, std::string> default_property_values = {
            {HTCondorServiceProperty::STOP_DAEMON_MESSAGE_PAYLOAD,                    "1024"},
            {HTCondorServiceProperty::DAEMON_STOPPED_MESSAGE_PAYLOAD,                 "1024"},
            {HTCondorServiceProperty::SUBMIT_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD,    "1024"},
            {HTCondorServiceProperty::SUBMIT_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD,     "1024"},
            {HTCondorServiceProperty::JOB_TYPE_NOT_SUPPORTED_MESSAGE_PAYLOAD,         "1024"},
            {HTCondorServiceProperty::NOT_ENOUGH_CORES_MESSAGE_PAYLOAD,               "1024"},
            {HTCondorServiceProperty::STANDARD_JOB_DONE_MESSAGE_PAYLOAD,              "1024"},
            {HTCondorServiceProperty::STANDARD_JOB_FAILED_MESSAGE_PAYLOAD,            "1024"},
            {HTCondorServiceProperty::TERMINATE_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD, "1024"},
            {HTCondorServiceProperty::TERMINATE_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD,  "1024"},
            {HTCondorServiceProperty::SUBMIT_PILOT_JOB_REQUEST_MESSAGE_PAYLOAD,       "1024"},
            {HTCondorServiceProperty::SUBMIT_PILOT_JOB_ANSWER_MESSAGE_PAYLOAD,        "1024"},
            {HTCondorServiceProperty::PILOT_JOB_STARTED_MESSAGE_PAYLOAD,              "1024"},
            {HTCondorServiceProperty::PILOT_JOB_EXPIRED_MESSAGE_PAYLOAD,              "1024"},
            {HTCondorServiceProperty::PILOT_JOB_FAILED_MESSAGE_PAYLOAD,               "1024"},
            {HTCondorServiceProperty::TERMINATE_PILOT_JOB_REQUEST_MESSAGE_PAYLOAD,    "1024"},
            {HTCondorServiceProperty::TERMINATE_PILOT_JOB_ANSWER_MESSAGE_PAYLOAD,     "1024"},
            {HTCondorServiceProperty::NUM_IDLE_CORES_REQUEST_MESSAGE_PAYLOAD,         "1024"},
            {HTCondorServiceProperty::NUM_IDLE_CORES_ANSWER_MESSAGE_PAYLOAD,          "1024"},
            {HTCondorServiceProperty::NUM_CORES_REQUEST_MESSAGE_PAYLOAD,              "1024"},
            {HTCondorServiceProperty::NUM_CORES_ANSWER_MESSAGE_PAYLOAD,               "1024"},
            {HTCondorServiceProperty::TTL_REQUEST_MESSAGE_PAYLOAD,                    "1024"},
            {HTCondorServiceProperty::TTL_ANSWER_MESSAGE_PAYLOAD,                     "1024"},
            {HTCondorServiceProperty::FLOP_RATE_REQUEST_MESSAGE_PAYLOAD,              "1024"},
            {HTCondorServiceProperty::FLOP_RATE_ANSWER_MESSAGE_PAYLOAD,               "1024"},
            {HTCondorServiceProperty::THREAD_STARTUP_OVERHEAD,                        "0.0"},
            {HTCondorServiceProperty::JOB_SELECTION_POLICY,                           "FCFS"},
            {HTCondorServiceProperty::RESOURCE_ALLOCATION_POLICY,                     "aggressive"},
            {HTCondorServiceProperty::TASK_SCHEDULING_CORE_ALLOCATION_ALGORITHM,      "maximum"},
            {HTCondorServiceProperty::TASK_SCHEDULING_TASK_SELECTION_ALGORITHM,       "maximum_flops"},
            {HTCondorServiceProperty::TASK_SCHEDULING_HOST_SELECTION_ALGORITHM,       "best_fit"},
    };


public:
    // Public Constructor
    HTCondor(const std::string &hostname,
                                     bool supports_standard_jobs,
                                     bool supports_pilot_jobs,
                                     std::set<std::pair<std::string, unsigned long>> compute_resources,
                                     wrench::StorageService *default_storage_service,
                                     std::map<std::string, std::string> plist = {});

    // Running jobs
    void submitStandardJob(wrench::StandardJob *job, std::map<std::string, std::string> &service_specific_args) override;

    void submitPilotJob(wrench::PilotJob *job, std::map<std::string, std::string> &service_specific_args) override;

    // Terminating jobs
    void terminateStandardJob(wrench::StandardJob *job) override;

    void terminatePilotJob(wrench::PilotJob *job) override;


    // Getting information
    double getTTL() override;

    double getCoreFlopRate() override;


    ~MultihostMulticoreComputeService();

private:
    friend class Simulation;

    int main() override;

    bool processNextMessage();

    void processGetNumCores(const std::string &answer_mailbox) override;

    void processGetNumIdleCores(const std::string &answer_mailbox) override;

    void processCreateVM(const std::string &answer_mailbox,
                         const std::string &pm_hostname,
                         const std::string &vm_hostname,
                         int num_cores,
                         bool supports_standard_jobs,
                         bool supports_pilot_jobs,
                         std::map <std::string, std::string> plist);

    void processSubmitStandardJob(const std::string &answer_mailbox, wrench::StandardJob *job,
                                  std::map <std::string, std::string> &service_specific_args) override;

    void terminate();

    /** @brief A map of VMs described by the VM actor, the actual compute service, and the total number of cores */
    std::map<std::string, std::tuple<simgrid::s4u::VirtualMachine *, std::unique_ptr < ComputeService>, int>> vm_list;
};

#endif //PROJECT_HTCONDOR_H
