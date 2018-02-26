
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
   // double getTTL();

    //double getCoreFlopRate();


    ~HTCondor();

private:
    friend class wrench::Simulation;

    HTCondor(const std::string &hostname, bool supports_standard_jobs, bool supports_pilot_jobs,
             std::set<std::pair<std::string, unsigned long>> compute_resources,
             std::map<std::string, std::string> plist, double ttl, wrench::PilotJob *pj, std::string suffix,
             wrench::StorageService *default_storage_service);


    int main() override;

    bool processNextMessage();

    //void processGetNumCores(const std::string &answer_mailbox);

    //void processGetNumIdleCores(const std::string &answer_mailbox);

    void processCreateVM(const std::string &answer_mailbox,
                         const std::string &pm_hostname,
                         const std::string &vm_hostname,
                         int num_cores,
                         bool supports_standard_jobs,
                         bool supports_pilot_jobs,
                         std::map <std::string, std::string> plist);

    void processSubmitStandardJob(const std::string &answer_mailbox, wrench::StandardJob *job,
                                  std::map <std::string, std::string> &service_specific_args) override;

    std::set<std::pair<std::string, unsigned long>> compute_resources;
    // Core availabilities (for each hosts, how many cores are currently available on it)
    std::map<std::string, unsigned long> core_availabilities;
    unsigned long total_num_cores;

    double ttl;
    bool has_ttl;
    double death_date;
    wrench::Alarm *death_alarm = nullptr;

    wrench::PilotJob *containing_pilot_job; // In case this service is in fact a pilot job

    // Set of current standard job executors
    std::set<std::unique_ptr<wrench::StandardJobExecutor>> standard_job_executors;
    // Set of completed standard job executors
    std::set<std::unique_ptr<wrench::StandardJobExecutor>> completed_job_executors;

    // Set of running jobs
    std::set<wrench::WorkflowJob *> running_jobs;

    // Queue of pending jobs (standard or pilot) that haven't begun executing
    std::deque<wrench::WorkflowJob *> pending_jobs;

    // Helper functions to make main() a bit more palatable

    void terminate(bool notify_pilot_job_submitters);

    void terminateAllPilotJobs();

    void failCurrentStandardJobs(std::shared_ptr<wrench::FailureCause> cause);

    void processStandardJobCompletion(wrench::StandardJobExecutor *executor, wrench::StandardJob *job);

    void processStandardJobFailure(wrench::StandardJobExecutor *executor,
                                   wrench::StandardJob *job,
                                   std::shared_ptr<wrench::FailureCause> cause);

    void processPilotJobCompletion(wrench::PilotJob *job);

    void processStandardJobTerminationRequest(wrench::StandardJob *job, std::string answer_mailbox);

    void processPilotJobTerminationRequest(wrench::PilotJob *job, std::string answer_mailbox);

    bool dispatchNextPendingJob();

    bool dispatchStandardJob(wrench::StandardJob *job);

    bool dispatchPilotJob(wrench::PilotJob *job);

    std::set<std::pair<std::string, unsigned long>> computeResourceAllocation(wrench::StandardJob *job);

    std::set<std::pair<std::string, unsigned long>> computeResourceAllocationAggressive(wrench::StandardJob *job);

//        void createWorkForNewlyDispatchedJob(StandardJob *job);

    void terminateRunningStandardJob(wrench::StandardJob *job);

    void terminateRunningPilotJob(wrench::PilotJob *job);

    void failPendingStandardJob(wrench::StandardJob *job, std::shared_ptr<wrench::FailureCause> cause);

    void failRunningStandardJob(wrench::StandardJob *job, std::shared_ptr<wrench::FailureCause> cause);

//        void processGetNumCores(const std::string &answer_mailbox) override;

    void processGetResourceInformation(const std::string &answer_mailbox) override;

//        void processGetNumIdleCores(const std::string &answer_mailbox) override;

    void processSubmitPilotJob(const std::string &answer_mailbox, wrench::PilotJob *job) override;
    /** @brief A map of VMs described by the VM actor, the actual compute service, and the total number of cores */
    std::map<std::string, std::tuple<simgrid::s4u::VirtualMachine *, std::unique_ptr < ComputeService>, int>> vm_list;
};

#endif //PROJECT_HTCONDOR_H
