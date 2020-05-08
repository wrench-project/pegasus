cmake_minimum_required(VERSION 3.2)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")

project(pegasus CXX)

add_definitions("-Wall -Wno-unused-variable -Wno-unused-private-field")

set(CMAKE_CXX_STANDARD 11)

# build the version number
set(WRENCH_PEGASUS_VERSION_MAJOR "1")
set(WRENCH_PEGASUS_VERSION_MINOR "6")
set(WRENCH_PEGASUS_VERSION_PATCH "0")
set(WRENCH_PEGASUS_VERSION_EXTRA "")

if (${WRENCH_PEGASUS_VERSION_PATCH} EQUAL "0")
    set(WRENCH_PEGASUS_RELEASE_VERSION "${WRENCH_PEGASUS_VERSION_MAJOR}.${WRENCH_PEGASUS_VERSION_MINOR}")
else ()
    set(WRENCH_PEGASUS_RELEASE_VERSION "$(WRENCH_PEGASUS_VERSION_MAJOR}.${WRENCH_PEGASUS_VERSION_MINOR}.${WRENCH_PEGASUS_VERSION_PATCH}")
endif ()

if (NOT ${WRENCH_PEGASUS_VERSION_EXTRA} EQUAL "")
    set(WRENCH_PEGASUS_RELEASE_VERSION "${WRENCH_PEGASUS_RELEASE_VERSION}-${WRENCH_PEGASUS_VERSION_EXTRA}")
endif ()

include_directories(src/ include/ /usr/local/include /usr/local/include/wrench /usr/local/include/wrench/tools/pegasus)

# source files
set(SOURCE_FILES
        src/DAGMan.h
        src/DAGMan.cpp
        src/DAGManMonitor.h
        src/DAGManMonitor.cpp
        src/DAGManScheduler.h
        src/DAGManScheduler.cpp
        src/SimulationConfig.h
        src/SimulationConfig.cpp
        src/PegasusSimulationTimestampTypes.h
        src/PegasusSimulationTimestampTypes.cpp
        src/PegasusRun.cpp
        src/PowerMeter.h
        src/PowerMeter.cpp
        )

set(TEST_FILES
        )

# wrench libraries
find_library(WRENCH_LIBRARY NAMES wrench)
find_library(WRENCH_PEGASUS_LIBRARY NAMES wrenchpegasusworkflowparser)
find_library(SIMGRID_LIBRARY NAMES simgrid)
find_library(PUGIXML_LIBRARY NAMES pugixml)
find_library(GTEST_LIBRARY NAMES gtest)

add_executable(wrench-pegasus-run ${SOURCE_FILES})
target_link_libraries(wrench-pegasus-run ${WRENCH_LIBRARY} ${WRENCH_PEGASUS_LIBRARY} ${SIMGRID_LIBRARY} ${PUGIXML_LIBRARY})
install(TARGETS wrench-pegasus-run DESTINATION bin)
