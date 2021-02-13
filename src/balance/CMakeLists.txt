CMAKE_MINIMUM_REQUIRED(VERSION 3.15)
PROJECT(TDengine)

add_library(balance 
  src/bnMain.cpp
  src/bnScore.cpp
  src/bnThread.cpp)

target_include_directories(balance
PRIVATE
	inc
)
target_link_libraries(balance mnode)
