CMAKE_MINIMUM_REQUIRED(VERSION 3.15)
PROJECT(TDengine)

ADD_EXECUTABLE(rclient rclient.cpp)
TARGET_LINK_LIBRARIES(rclient trpc)

ADD_EXECUTABLE(rsclient rsclient.cpp)
TARGET_LINK_LIBRARIES(rsclient trpc)

ADD_EXECUTABLE(rserver rserver.cpp)
TARGET_LINK_LIBRARIES(rserver trpc)