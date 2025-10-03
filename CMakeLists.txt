cmake_minimum_required(VERSION 3.0.2)
project(advanced_bag_recorder)

if(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "aarch64")
  set(CPU_TYPE "aarch64")
elseif(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "x86_64")
  set(CPU_TYPE "x86")
else()
  set(CPU_TYPE "unknown")
  message("Unknown CPU type!")
endif()
message("********* CPU: ${CPU_TYPE} *********")

add_compile_options(-std=c++17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

if(CPU_TYPE STREQUAL "x86")
    set(CMAKE_BUILD_TYPE Debug)
    find_package(OpenCV REQUIRED)
    message("build type: Debug")
endif()

if(CPU_TYPE STREQUAL "aarch64")
    set(CMAKE_BUILD_TYPE Release)
    find_package(OpenCV 4 REQUIRED)
    message("build type: Release")
endif()

find_package(catkin REQUIRED COMPONENTS
  roscpp
  rosbag
  std_msgs
  nav_msgs
  sensor_msgs
  cv_bridge
  image_transport
  topic_tools
  geometry_msgs
  tf2_msgs
  visualization_msgs
)

find_package(OpenCV REQUIRED)

catkin_package(
  INCLUDE_DIRS include
  CATKIN_DEPENDS roscpp rosbag std_msgs nav_msgs sensor_msgs cv_bridge topic_tools geometry_msgs tf2_msgs visualization_msgs
)

include_directories(
  include
  ${catkin_INCLUDE_DIRS}
  ${OpenCV_INCLUDE_DIRS}
)

add_executable(advanced_bag_recorder_node src/advanced_bag_recorder_node.cpp src/advanced_bag_recorder.cpp)
target_link_libraries(advanced_bag_recorder_node ${catkin_LIBRARIES} ${OpenCV_LIBS})


#install
install(TARGETS ${PROJECT_NAME}_node
    ARCHIVE DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION} 
    LIBRARY DESTINATION ${CATKIN_PACKAGE_LIB_DESTINATION} 
    RUNTIME DESTINATION ${CATKIN_PACKAGE_BIN_DESTINATION})
install(DIRECTORY launch/ DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/launch)
install(DIRECTORY params/ DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/params)
install(DIRECTORY version/ DESTINATION ${CATKIN_PACKAGE_SHARE_DESTINATION}/version)