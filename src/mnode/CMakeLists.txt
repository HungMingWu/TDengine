CMAKE_MINIMUM_REQUIRED(VERSION 3.15)
PROJECT(TDengine)

AUX_SOURCE_DIRECTORY(src SRC)

ADD_LIBRARY(mnode ${SRC})
target_include_directories(mnode
PUBLIC
	inc
PRIVATE
	../dnode/inc
)