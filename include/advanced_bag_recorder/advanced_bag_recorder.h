#pragma once
#include <ros/ros.h>
#include <rosbag/bag.h>
#include <nav_msgs/OccupancyGrid.h>
#include <sensor_msgs/CompressedImage.h>
#include <topic_tools/shape_shifter.h>
#include <cv_bridge/cv_bridge.h>
#include <opencv2/opencv.hpp>
#include <boost/filesystem.hpp>
#include <deque>
#include <string>
#include <unordered_map>

namespace fs = boost::filesystem;

struct BagInfo {
    std::string filename;
    ros::Time start_time;
};


class AdvancedBagRecorder
{
public:
    explicit AdvancedBagRecorder(ros::NodeHandle& nh);

private:
    void loadParams();
    void manageFolders();
    bool checkFreeSpaceOrExit();
    void splitBagCallback(const ros::TimerEvent&);
    void genericCallback(const topic_tools::ShapeShifter::ConstPtr& msg, const std::string& topic);
    void costmapCallback(const nav_msgs::OccupancyGrid::ConstPtr& msg);
    void openNewBag();
    void closeCurrentBag();
    void removeOldBagsIfNeeded();

private:
    ros::NodeHandle nh_;
    ros::NodeHandle pnh_;
    rosbag::Bag current_bag_;
    std::deque<BagInfo> bags_;
    ros::Timer timer_;
    ros::Subscriber sub_costmap_;
    std::vector<ros::Subscriber> subs_;

    std::vector<std::string> normal_topics_;
    std::unordered_map<std::string, int> topic_max_msgs_map_;
    std::unordered_map<std::string, int> topic_msg_count_;

    double split_time_;
    int max_bags_;
    std::string costmap_topic_;
    std::string costmap_output_topic_;
    double costmap_save_rate_;
    int costmap_jpeg_quality_;
    ros::Time last_costmap_time_;

    std::string bag_root_path_;
    int max_folders_;
    std::string current_folder_;
    double min_free_space_gb_;

    int frame_idx_;
};
