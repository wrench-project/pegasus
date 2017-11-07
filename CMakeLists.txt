cmake_minimum_required(VERSION 3.2)
message(STATUS "Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}.${CMAKE_PATCH_VERSION}")

project(pegasus CXX)

add_definitions("-Wall -Wno-unused-variable -Wno-unused-private-field")

set(CMAKE_CXX_STANDARD 11)

# build the version number
set(PEGASUS_VERSION_MAJOR "1")


include_directories(src/ include/ /usr/local/include /usr/local/include/wrench )
#include_directories(src/wrench/ include/ /usr/include /usr/local/include /Users/casanova/Home/PROJECTS/SIMGRID/simgrid /opt/local/include /usr/local/include/nlohmann)


# library header files
set(HEADER_FILES
        include/DagMan.h
        include/HTCondor.h
        )

# source files
set(SOURCE_FILES
        src/DagMan.cpp
        src/HTCondor.cpp
        )

set(TEST_FILES
        PegasusRun.cpp)

# wrench library
find_library(WRENCH_LIBRARY NAMES wrench)


add_library(pegasus STATIC ${SOURCE_FILES})
target_link_libraries(pegasus ${WRENCH_LIBRARY})

install(TARGETS pegasus DESTINATION lib)
install(DIRECTORY include/ DESTINATION include)

add_executable(unit_tests ${SOURCE_FILES} ${HEADER_FILES} ${TEST_FILES})

