# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/home/lbh/git/LeanStoreOOPW/build/share_libs/lz4/LibLz4-prefix/src/LibLz4"
  "/home/lbh/git/LeanStoreOOPW/build/share_libs/lz4/LibLz4-prefix/src/LibLz4-build"
  "/home/lbh/git/LeanStoreOOPW/build/share_libs/lz4/LibLz4-prefix"
  "/home/lbh/git/LeanStoreOOPW/build/share_libs/lz4/LibLz4-prefix/tmp"
  "/home/lbh/git/LeanStoreOOPW/build/share_libs/lz4/LibLz4-prefix/src/LibLz4-stamp"
  "/home/lbh/git/LeanStoreOOPW/build/share_libs/lz4/LibLz4-prefix/src"
  "/home/lbh/git/LeanStoreOOPW/build/share_libs/lz4/LibLz4-prefix/src/LibLz4-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/home/lbh/git/LeanStoreOOPW/build/share_libs/lz4/LibLz4-prefix/src/LibLz4-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/home/lbh/git/LeanStoreOOPW/build/share_libs/lz4/LibLz4-prefix/src/LibLz4-stamp${cfgdir}") # cfgdir has leading slash
endif()
