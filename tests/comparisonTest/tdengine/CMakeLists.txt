CMAKE_MINIMUM_REQUIRED(VERSION 3.15)
PROJECT(TDengine)

IF (TD_LINUX)
  add_executable(tdengineTest tdengineTest.cpp)
  target_link_libraries(tdengineTest taos_static tutil common pthread)
ENDIF()
