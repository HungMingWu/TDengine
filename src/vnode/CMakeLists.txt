CMAKE_MINIMUM_REQUIRED(VERSION 3.15)
PROJECT(TDengine)

IF (TD_LINUX)
  AUX_SOURCE_DIRECTORY(src SRC)
  ADD_LIBRARY(vnode ${SRC})
  target_include_directories(vnode
  PUBLIC
	${TD_COMMUNITY_DIR}/src/query/inc
	${TD_COMMUNITY_DIR}/src/tsdb/inc
	${TD_COMMUNITY_DIR}/src/dnode/inc
	${TD_COMMUNITY_DIR}/src/sync/inc
	${TD_ENTERPRISE_DIR}/src/inc
	inc
  )
  TARGET_LINK_LIBRARIES(vnode tsdb tcq twal)
ENDIF ()
