cmake_minimum_required(VERSION 3.0.2)
project(advanced_bag_recorder)

add_compile_options(-std=c++14)

find_package(catkin REQUIRED COMPONENTS
  roscpp
  rosbag
  std_msgs
  nav_msgs
  sensor_msgs
  cv_bridge
  image_transport
  topic_tools
)

find_package(OpenCV REQUIRED)

catkin_package(
  INCLUDE_DIRS include
  CATKIN_DEPENDS roscpp rosbag std_msgs nav_msgs sensor_msgs cv_bridge topic_tools
)

include_directories(
  include
  ${catkin_INCLUDE_DIRS}
  ${OpenCV_INCLUDE_DIRS}
)

add_executable(advanced_bag_recorder_node src/advanced_bag_recorder_node.cpp)
target_link_libraries(advanced_bag_recorder_node ${catkin_LIBRARIES} ${OpenCV_LIBS})
