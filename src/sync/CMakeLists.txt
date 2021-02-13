CMAKE_MINIMUM_REQUIRED(VERSION 3.15)
PROJECT(TDengine)

AUX_SOURCE_DIRECTORY(src SRC)
  
IF (TD_LINUX)
  LIST(REMOVE_ITEM SRC src/syncArbitrator.cpp)
  ADD_LIBRARY(sync ${SRC})
  target_include_directories(sync PUBLIC
	inc/
  )
  TARGET_LINK_LIBRARIES(sync tutil pthread common twal)

  LIST(APPEND BIN_SRC src/syncArbitrator.cpp)
  LIST(APPEND BIN_SRC src/syncTcp.cpp)
  ADD_EXECUTABLE(tarbitrator ${BIN_SRC})
  TARGET_LINK_LIBRARIES(tarbitrator sync common osdetail tutil)

  ADD_SUBDIRECTORY(test)
ENDIF ()
