# - find LibZip compression library
# ZIP_INCLUDE_DIR - Where to find LibZip compression library header files (directory)
# ZIP_LIBRARY - LibZip compression library libraries
# ZIP_LIBRARY_RELEASE - Where the release library is
# ZIP_LIBRARY_DEBUG - Where the debug library is
# ZIP_FOUND - Set to TRUE if we found everything (library, includes and executable)

# Copyright (c) 2012 Adam Ross, <example@example.com>
#
# Redistribution and use is allowed according to the terms of the BSD license.
# For details see the accompanying COPYING-CMAKE-SCRIPTS file.
#
# Generated by CModuler, a CMake Module Generator - http://gitorious.org/cmoduler

IF( ZIP_INCLUDE_DIR AND ZIP_LIBRARY_RELEASE AND ZIP_LIBRARY_DEBUG )
    SET(ZIP_FIND_QUIETLY TRUE)
ENDIF( ZIP_INCLUDE_DIR AND ZIP_LIBRARY_RELEASE AND ZIP_LIBRARY_DEBUG )

FIND_PATH( ZIP_INCLUDE_DIR zip.h  )

FIND_LIBRARY(ZIP_LIBRARY_RELEASE NAMES zip )

FIND_LIBRARY(ZIP_LIBRARY_DEBUG NAMES   HINTS /usr/lib/debug/usr/lib/ )

IF( ZIP_LIBRARY_RELEASE OR ZIP_LIBRARY_DEBUG AND ZIP_INCLUDE_DIR )
	SET( ZIP_FOUND TRUE )
ENDIF( ZIP_LIBRARY_RELEASE OR ZIP_LIBRARY_DEBUG AND ZIP_INCLUDE_DIR )

IF( ZIP_LIBRARY_DEBUG AND ZIP_LIBRARY_RELEASE )
	# if the generator supports configuration types then set
	# optimized and debug libraries, or if the CMAKE_BUILD_TYPE has a value
	IF( CMAKE_CONFIGURATION_TYPES OR CMAKE_BUILD_TYPE )
		SET( ZIP_LIBRARY optimized ${ZIP_LIBRARY_RELEASE} debug ${ZIP_LIBRARY_DEBUG} )
	ELSE( CMAKE_CONFIGURATION_TYPES OR CMAKE_BUILD_TYPE )
    # if there are no configuration types and CMAKE_BUILD_TYPE has no value
    # then just use the release libraries
		SET( ZIP_LIBRARY ${ZIP_LIBRARY_RELEASE} )
	ENDIF( CMAKE_CONFIGURATION_TYPES OR CMAKE_BUILD_TYPE )
ELSEIF( ZIP_LIBRARY_RELEASE )
	SET( ZIP_LIBRARY ${ZIP_LIBRARY_RELEASE} )
ELSE( ZIP_LIBRARY_DEBUG AND ZIP_LIBRARY_RELEASE )
	SET( ZIP_LIBRARY ${ZIP_LIBRARY_DEBUG} )
ENDIF( ZIP_LIBRARY_DEBUG AND ZIP_LIBRARY_RELEASE )

IF( ZIP_FOUND )
	IF( NOT ZIP_FIND_QUIETLY )
		MESSAGE( STATUS "Found Zip header file in ${ZIP_INCLUDE_DIR}")
		MESSAGE( STATUS "Found Zip libraries: ${ZIP_LIBRARY}")
	ENDIF( NOT ZIP_FIND_QUIETLY )
ELSE(ZIP_FOUND)
	IF( ZIP_FIND_REQUIRED)
		MESSAGE( FATAL_ERROR "Could not find Zip" )
	ELSE( ZIP_FIND_REQUIRED)
		MESSAGE( STATUS "Optional package Zip was not found" )
	ENDIF( ZIP_FIND_REQUIRED)
ENDIF(ZIP_FOUND)