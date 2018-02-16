//
// Created by james oeth on 12/12/17.
//

#ifndef PEGASUS_HTCONDORCOMPUTESERVICE_H
#define PEGASUS_HTCONDORCOMPUTESERVICE_H

/**
 * @brief A Cloud Service
 */
class HTCondorService : public ComputeService {

private:
    std::map <std::string, std::string> default_property_values =
            {{HTCondorServiceProperty::STOP_DAEMON_MESSAGE_PAYLOAD,                 "1024"},
             {HTCondorServiceProperty::DAEMON_STOPPED_MESSAGE_PAYLOAD,              "1024"},
             {HTCondorServiceProperty::NUM_IDLE_CORES_REQUEST_MESSAGE_PAYLOAD,      "1024"},
             {HTCondorServiceProperty::NUM_IDLE_CORES_ANSWER_MESSAGE_PAYLOAD,       "1024"},
             {HTCondorServiceProperty::NUM_CORES_REQUEST_MESSAGE_PAYLOAD,           "1024"},
             {HTCondorServiceProperty::NUM_CORES_ANSWER_MESSAGE_PAYLOAD,            "1024"},
             {HTCondorServiceProperty::CREATE_VM_REQUEST_MESSAGE_PAYLOAD,           "1024"},
             {HTCondorServiceProperty::CREATE_VM_ANSWER_MESSAGE_PAYLOAD,            "1024"},
             {HTCondorServiceProperty::SUBMIT_STANDARD_JOB_REQUEST_MESSAGE_PAYLOAD, "1024"},
             {HTCondorServiceProperty::SUBMIT_STANDARD_JOB_ANSWER_MESSAGE_PAYLOAD,  "1024"}
            };

public:
    HTCondorService(const std::string &hostname,
                 bool supports_standard_jobs,
                 bool supports_pilot_jobs,
                 StorageService *default_storage_service,
                 std::map <std::string, std::string> plist);

    /***********************/
    /** \cond DEVELOPER    */
    /***********************/

    bool createVM(const std::string &pm_hostname,
                  const std::string &vm_hostname,
                  unsigned long num_cores,
                  std::map <std::string, std::string> plist = {});

    // Running jobs
    void submitStandardJob(StandardJob *job, std::map <std::string, std::string> &service_specific_args) override;

    void submitPilotJob(PilotJob *job, std::map <std::string, std::string> &service_specific_args) override;

    /* uncomment these when implementing for multihost
    double getTTL() override;

    double getCoreFlopRate() override;
     */

    /***********************/
    /** \endcond          **/
    /***********************/

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

    void processSubmitStandardJob(const std::string &answer_mailbox, StandardJob *job,
                                  std::map <std::string, std::string> &service_specific_args) override;

    void terminate();

    /** @brief A map of VMs described by the VM actor, the actual compute service, and the total number of cores */
    std::map<std::string, std::tuple<simgrid::s4u::VirtualMachine *, std::unique_ptr < ComputeService>, int>> vm_list;
};


#endif //PEGASUS_HTCONDORCOMPUTESERVICE_H
