CMAKE_MINIMUM_REQUIRED(VERSION 3.15)
PROJECT(TDengine)

add_library(nonstd INTERFACE)
target_include_directories(nonstd 
INTERFACE
	./
)