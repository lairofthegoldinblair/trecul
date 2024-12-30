# - Try to find an mpi library (e.g. http://www.mpich.org/)
#
# Once done this will define
#
#  MPI_FOUND - system has libmpi
#  MPI_INCLUDE_DIR - the libmpi include directory
#  MPI_LIBRARIES - Link these to use libmpi
#

# Copyright (c) 2009, Patrick Spendrin, <ps_ml@gmx.de>
#
# Redistribution and use is allowed according to the terms of the BSD license, as applying for all kdelibs cmake modules


if (MPI_INCLUDE_DIR AND MPI_LIBRARIES AND MPIEXEC_EXECUTABLE)

    # in cache already
    set(MPI_FOUND TRUE)

else (MPI_INCLUDE_DIR AND MPI_LIBRARIES AND MPIEXEC_EXECUTABLE)

    find_path(MPI_INCLUDE_DIR mpi.h)

    find_library(MPI_LIBRARIES NAMES mpi libmpi)

    find_program(MPIEXEC_EXECUTABLE mpiexec)

    if (MPI_INCLUDE_DIR AND MPI_LIBRARIES AND MPIEXEC_EXECUTABLE)
        set(MPI_FOUND TRUE)
        # TODO version check is missing
    endif (MPI_INCLUDE_DIR AND MPI_LIBRARIES AND MPIEXEC_EXECUTABLE)

    if (MPI_FOUND)
        if (NOT MPI_FIND_QUIETLY)
            message(STATUS "Found MPI C runtime: ${MPI_LIBRARIES}")
        endif (NOT MPI_FIND_QUIETLY)
    else (MPI_FOUND)
        if (MPI_FIND_REQUIRED)
            if (NOT MPI_INCLUDE_DIR)
                message(FATAL_ERROR "Could NOT find MPI header files")
            endif (NOT MPI_INCLUDE_DIR)
            if (NOT MPI_LIBRARIES)
                message(FATAL_ERROR "Could NOT find MPI C runtime library")
            endif (NOT MPI_LIBRARIES)
            if (NOT MPIEXEC_EXECUTABLE)
                message(FATAL_ERROR "Could NOT find MPI mpiexec executable")
            endif (NOT MPIEXEC_EXECUTABLE)
        endif (MPI_FIND_REQUIRED)
    endif (MPI_FOUND)

    mark_as_advanced(MPI_INCLUDE_DIR MPI_LIBRARIES MPIEXEC_EXECUTABLE)
  
endif (MPI_INCLUDE_DIR AND MPI_LIBRARIES AND MPIEXEC_EXECUTABLE)
