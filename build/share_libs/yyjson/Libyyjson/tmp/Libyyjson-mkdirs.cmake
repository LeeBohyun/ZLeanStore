# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/home/lbh/git/LeanStoreOOPW/build/share_libs/yyjson/Libyyjson/src/Libyyjson"
  "/home/lbh/git/LeanStoreOOPW/build/share_libs/yyjson/Libyyjson/src/Libyyjson-build"
  "/home/lbh/git/LeanStoreOOPW/build/share_libs/yyjson/Libyyjson"
  "/home/lbh/git/LeanStoreOOPW/build/share_libs/yyjson/Libyyjson/tmp"
  "/home/lbh/git/LeanStoreOOPW/build/share_libs/yyjson/Libyyjson/src/Libyyjson-stamp"
  "/home/lbh/git/LeanStoreOOPW/build/share_libs/yyjson/Libyyjson/src"
  "/home/lbh/git/LeanStoreOOPW/build/share_libs/yyjson/Libyyjson/src/Libyyjson-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/home/lbh/git/LeanStoreOOPW/build/share_libs/yyjson/Libyyjson/src/Libyyjson-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/home/lbh/git/LeanStoreOOPW/build/share_libs/yyjson/Libyyjson/src/Libyyjson-stamp${cfgdir}") # cfgdir has leading slash
endif()
