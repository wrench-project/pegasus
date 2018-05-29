/**
 * Copyright (c) 2017-2018. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef PEGASUS_HTCONDORSERVICEMESSAGEPAYLOAD_H
#define PEGASUS_HTCONDORSERVICEMESSAGEPAYLOAD_H

#include "wrench-dev.h"

namespace wrench {
    namespace pegasus {

        /**
         * @brief Configurable message payloads for a CloudService
         */
        class HTCondorServiceMessagePayload : public ComputeServiceMessagePayload {

        };
    }
}

#endif //PEGASUS_HTCONDORSERVICEMESSAGEPAYLOAD_H
