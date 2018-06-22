/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef PEGASUS_HTCONDORCENTRALMANAGERSERVICEMESSAGEPAYLOAD_H
#define PEGASUS_HTCONDORCENTRALMANAGERSERVICEMESSAGEPAYLOAD_H

#include "wrench-dev.h"

namespace wrench {
    namespace pegasus {

        /**
         * @brief Configurable message payloads for an HTCondor Central Manager service
         */
        class HTCondorCentralManagerServiceMessagePayload : public ComputeServiceMessagePayload {
        public:
            /** @brief The number of bytes in the control message sent by the daemon to state that the negotiator has been completed **/
            DECLARE_MESSAGEPAYLOAD_NAME(HTCONDOR_NEGOTIATOR_DONE_MESSAGE_PAYLOAD);
        };
    }
}

#endif //PEGASUS_HTCONDORCENTRALMANAGERSERVICEMESSAGEPAYLOAD_H
