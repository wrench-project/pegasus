/**
 * Copyright (c) 2017-2019. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#ifndef PEGASUS_POWERMETER_H
#define PEGASUS_POWERMETER_H

#include <wrench-dev.h>

namespace wrench {
    namespace pegasus {

        class DAGMan;

        class PowerMeter : public Service {
        public:
            void kill();

            void stop();

        protected:
            friend class DAGMan;

        private:
            int main();
            bool processNextMessage(double timeout);
        };
    }
}

#endif //PEGASUS_POWERMETER_H
