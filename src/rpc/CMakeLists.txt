CMAKE_MINIMUM_REQUIRED(VERSION 3.15)
PROJECT(TDengine)

AUX_SOURCE_DIRECTORY(src SRC)

add_library(trpc ${SRC})

target_link_libraries(trpc 
PRIVATE
	tutil
	lz4
	common
)

target_include_directories(trpc
PUBLIC
	inc
)

IF (TD_LINUX)
add_subdirectory(test)
ENDIF ()
