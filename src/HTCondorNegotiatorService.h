/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef PEGASUS_HTCONDORNEGOTIATORSERVICE_H
#define PEGASUS_HTCONDORNEGOTIATORSERVICE_H

#include <wrench-dev.h>
#include "HTCondorCentralManagerServiceMessagePayload.h"

namespace wrench {
    namespace pegasus {

        /***********************/
        /** \cond DEVELOPER    */
        /***********************/
        class HTCondorNegotiatorService : public Service {
        private:
            std::map<std::string, std::string> default_messagepayload_values = {
                    {HTCondorCentralManagerServiceMessagePayload::STOP_DAEMON_MESSAGE_PAYLOAD,              "1024"},
                    {HTCondorCentralManagerServiceMessagePayload::DAEMON_STOPPED_MESSAGE_PAYLOAD,           "1024"},
                    {HTCondorCentralManagerServiceMessagePayload::HTCONDOR_NEGOTIATOR_DONE_MESSAGE_PAYLOAD, "1024"},
            };

        public:

            HTCondorNegotiatorService(std::string &hostname,
                                      std::set<std::shared_ptr<ComputeService>> &compute_resources,
                                      std::vector<StandardJob *> &pending_jobs,
                                      std::string &reply_mailbox);

            ~HTCondorNegotiatorService();

        private:
            int main() override;

            /** mailbox to reply **/
            std::string reply_mailbox;
            /** set of compute resources **/
            std::set<std::shared_ptr<ComputeService>> compute_resources;
            /** queue of pending jobs **/
            std::vector<StandardJob *> pending_jobs;
        };

        /***********************/
        /** \endcond           */
        /***********************/
    }
}

#endif //PEGASUS_HTCONDORNEGOTIATORSERVICE_H