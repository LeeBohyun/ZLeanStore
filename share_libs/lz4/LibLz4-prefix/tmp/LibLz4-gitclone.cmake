# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

if(EXISTS "/home/lbh/leanstore-vmcache-main/share_libs/lz4/LibLz4-prefix/src/LibLz4-stamp/LibLz4-gitclone-lastrun.txt" AND EXISTS "/home/lbh/leanstore-vmcache-main/share_libs/lz4/LibLz4-prefix/src/LibLz4-stamp/LibLz4-gitinfo.txt" AND
  "/home/lbh/leanstore-vmcache-main/share_libs/lz4/LibLz4-prefix/src/LibLz4-stamp/LibLz4-gitclone-lastrun.txt" IS_NEWER_THAN "/home/lbh/leanstore-vmcache-main/share_libs/lz4/LibLz4-prefix/src/LibLz4-stamp/LibLz4-gitinfo.txt")
  message(STATUS
    "Avoiding repeated git clone, stamp file is up to date: "
    "'/home/lbh/leanstore-vmcache-main/share_libs/lz4/LibLz4-prefix/src/LibLz4-stamp/LibLz4-gitclone-lastrun.txt'"
  )
  return()
endif()

execute_process(
  COMMAND ${CMAKE_COMMAND} -E rm -rf "/home/lbh/leanstore-vmcache-main/share_libs/lz4/LibLz4-prefix/src/LibLz4"
  RESULT_VARIABLE error_code
)
if(error_code)
  message(FATAL_ERROR "Failed to remove directory: '/home/lbh/leanstore-vmcache-main/share_libs/lz4/LibLz4-prefix/src/LibLz4'")
endif()

# try the clone 3 times in case there is an odd git clone issue
set(error_code 1)
set(number_of_tries 0)
while(error_code AND number_of_tries LESS 3)
  execute_process(
    COMMAND "/usr/bin/git" 
            clone --no-checkout --config "advice.detachedHead=false" "https://github.com/lz4/lz4.git" "LibLz4"
    WORKING_DIRECTORY "/home/lbh/leanstore-vmcache-main/share_libs/lz4/LibLz4-prefix/src"
    RESULT_VARIABLE error_code
  )
  math(EXPR number_of_tries "${number_of_tries} + 1")
endwhile()
if(number_of_tries GREATER 1)
  message(STATUS "Had to git clone more than once: ${number_of_tries} times.")
endif()
if(error_code)
  message(FATAL_ERROR "Failed to clone repository: 'https://github.com/lz4/lz4.git'")
endif()

execute_process(
  COMMAND "/usr/bin/git" 
          checkout "v1.8.3" --
  WORKING_DIRECTORY "/home/lbh/leanstore-vmcache-main/share_libs/lz4/LibLz4-prefix/src/LibLz4"
  RESULT_VARIABLE error_code
)
if(error_code)
  message(FATAL_ERROR "Failed to checkout tag: 'v1.8.3'")
endif()

set(init_submodules TRUE)
if(init_submodules)
  execute_process(
    COMMAND "/usr/bin/git" 
            submodule update --recursive --init 
    WORKING_DIRECTORY "/home/lbh/leanstore-vmcache-main/share_libs/lz4/LibLz4-prefix/src/LibLz4"
    RESULT_VARIABLE error_code
  )
endif()
if(error_code)
  message(FATAL_ERROR "Failed to update submodules in: '/home/lbh/leanstore-vmcache-main/share_libs/lz4/LibLz4-prefix/src/LibLz4'")
endif()

# Complete success, update the script-last-run stamp file:
#
execute_process(
  COMMAND ${CMAKE_COMMAND} -E copy "/home/lbh/leanstore-vmcache-main/share_libs/lz4/LibLz4-prefix/src/LibLz4-stamp/LibLz4-gitinfo.txt" "/home/lbh/leanstore-vmcache-main/share_libs/lz4/LibLz4-prefix/src/LibLz4-stamp/LibLz4-gitclone-lastrun.txt"
  RESULT_VARIABLE error_code
)
if(error_code)
  message(FATAL_ERROR "Failed to copy script-last-run stamp file: '/home/lbh/leanstore-vmcache-main/share_libs/lz4/LibLz4-prefix/src/LibLz4-stamp/LibLz4-gitclone-lastrun.txt'")
endif()
