CMAKE_MINIMUM_REQUIRED(VERSION 3.15)
PROJECT(TDengine)

ADD_SUBDIRECTORY(monitor)
ADD_SUBDIRECTORY(http)
IF (TD_MQTT)
  ADD_SUBDIRECTORY(mqtt)
ENDIF ()