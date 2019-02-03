/**
 * Copyright (c) 2017-2019. The WRENCH Team.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

#include "PowerMeter.h"

XBT_LOG_NEW_DEFAULT_CATEGORY(power_meter, "Log category for Power Meter");

namespace wrench {
    namespace pegasus {

        /**
         * @brief Main method of the daemon that implements the PowerMeter
         * @return 0 on success
         */
        int PowerMeter::main() {

          TerminalOutput::setThisProcessLoggingColor(TerminalOutput::COLOR_YELLOW);

          WRENCH_INFO("New Power Meter Manager starting (%s)", this->mailbox_name.c_str());

          WRENCH_INFO("Energy Meter Manager terminating");

          return 0;
        }

        /**
         * @brief Process the next message
         * @return true if the daemon should continue, false otherwise
         *
         * @throw std::runtime_error
         */
        bool PowerMeter::processNextMessage(double timeout) {

          std::unique_ptr<SimulationMessage> message = nullptr;

          if (message == nullptr) {
            WRENCH_INFO("Got a NULL message... Likely this means we're all done. Aborting!");
            return false;
          }

          WRENCH_INFO("Power Meter got a %s message", message->getName().c_str());

          if (auto msg = dynamic_cast<ServiceStopDaemonMessage *>(message.get())) {
            // There shouldn't be any need to clean any state up
            return false;

          } else {
            throw std::runtime_error(
                    "PowerMeter::waitForNextMessage(): Unexpected [" + message->getName() + "] message");
          }
        }

        /**
         * @brief Kill the power meter (brutally terminate the daemon)
         */
        void PowerMeter::kill() {
          this->killActor();
        }

        /**
         * @brief Stop the power meter
         *
         * @throw WorkflowExecutionException
         * @throw std::runtime_error
         */
        void PowerMeter::stop() {
          try {
            S4U_Mailbox::putMessage(this->mailbox_name, new ServiceStopDaemonMessage("", 0.0));
          } catch (std::shared_ptr<NetworkError> &cause) {
            throw WorkflowExecutionException(cause);
          }
        }

    }
}