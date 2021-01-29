CMAKE_MINIMUM_REQUIRED(VERSION 3.15)
PROJECT(TDengine)

AUX_SOURCE_DIRECTORY(src SRC)

add_library(common ${SRC})
target_include_directories(common
PRIVATE
	inc
)
target_link_libraries(common 
PRIVATE
	tutil
)
