include(ExternalProject)

# Decide how to build the external yyjson project
if(MSVC)
  # On MSVC, just use CMake's native build invocation
  set(YYJSON_BUILD_COMMAND
      ${CMAKE_COMMAND} --build . --config $<CONFIG>)
else()
  # On Unix-like systems, find a real 'make' program
  find_program(MAKE_EXECUTABLE NAMES gmake make)
  if(NOT MAKE_EXECUTABLE)
    message(FATAL_ERROR "Could not find 'make' required to build yyjson")
  endif()
  set(YYJSON_BUILD_COMMAND ${MAKE_EXECUTABLE})
endif()

ExternalProject_Add(
  Libyyjson
  GIT_REPOSITORY https://github.com/ibireme/yyjson.git
  GIT_TAG 0.7.0
  BUILD_COMMAND ${YYJSON_BUILD_COMMAND}
  PREFIX ${CMAKE_CURRENT_BINARY_DIR}/Libyyjson
  CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=<INSTALL_DIR>
)

ExternalProject_Get_Property(Libyyjson INSTALL_DIR)

add_library(yyjson INTERFACE)
target_link_libraries(yyjson INTERFACE ${INSTALL_DIR}/lib/libyyjson.a)
target_include_directories(yyjson INTERFACE ${INSTALL_DIR}/include)
add_dependencies(yyjson Libyyjson)
