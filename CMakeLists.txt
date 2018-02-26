cmake_minimum_required(VERSION 3.2)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")

project(pegasus CXX)

add_definitions("-Wall -Wno-unused-variable -Wno-unused-private-field")

set(CMAKE_CXX_STANDARD 11)

# build the version number
set(PEGASUS_VERSION_MAJOR "1")

include_directories(src/ include/ /usr/local/include /usr/local/include/wrench)

# source files
set(SOURCE_FILES
        src/DagMan.h
        src/DagMan.cpp
        src/HTCondor.h
        src/HTCondor.cpp
        src/PegasusRun.cpp
        src/HTCondorServiceMessage.h
        src/HTCondorServiceMessage.cpp
        src/HTCondorServiceProperty.h
        src/HTCondorServiceProperty.cpp
        )

set(TEST_FILES
        )

# wrench library
find_library(WRENCH_LIBRARY NAMES wrench)
find_library(SIMGRID_LIBRARY NAMES simgrid)
find_library(PUGIXML_LIBRARY NAMES pugixml)
find_library(LEMON_LIBRARY NAMES emon)
find_library(GTEST_LIBRARY NAMES gtest)

add_executable(pegasus-run ${SOURCE_FILES})
target_link_libraries(pegasus-run ${WRENCH_LIBRARY} ${SIMGRID_LIBRARY} ${PUGIXML_LIBRARY} ${LEMON_LIBRARY})
install(TARGETS pegasus-run DESTINATION bin)
