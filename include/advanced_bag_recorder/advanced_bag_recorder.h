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
#include <ctime>
#include <geometry_msgs/PolygonStamped.h>
#include <tf2_msgs/TFMessage.h>

namespace fs = boost::filesystem;

struct BagInfo
{
    std::string filename;
    ros::Time start_time;
};

class AdvancedBagRecorder
{
public:
    explicit AdvancedBagRecorder(ros::NodeHandle &nh);

private:
    void loadParams();
    void manageFolders();
    bool checkFreeSpaceOrExit();
    void splitBagCallback(const ros::TimerEvent &);
    void genericCallback(const topic_tools::ShapeShifter::ConstPtr &msg, const std::string &topic);
    void costmapCallback(const nav_msgs::OccupancyGrid::ConstPtr &msg);
    void openNewBag();
    void closeCurrentBag();
    void removeOldBagsIfNeeded();
    std::string getTimeString();
    void footprintCallback(const geometry_msgs::PolygonStamped::ConstPtr &msg);

private:
    ros::NodeHandle nh_;
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
    std::string footprint_topic_;
    double costmap_save_rate_;
    ros::Time last_costmap_time_;

    std::string bag_root_path_;
    int max_folders_;
    std::string current_folder_;
    double min_free_space_gb_;
    std::string current_time_str_; // 当前录制的日期时间字符串
    geometry_msgs::PolygonStamped latest_footprint_;
    std::mutex footprint_mutex_;
    bool has_latest_footprint_ = false; // 标记是否收到过数据
    ros::Publisher costmap_image_pub_;
    ros::Subscriber footprint_sub_;

    std::string map_topic_;
    std::string global_costmal_topic_;
    std::string tf_static_topic_;
    nav_msgs::OccupancyGrid::ConstPtr last_map_msg_;
    tf2_msgs::TFMessage::ConstPtr last_tf_static_msg_;
    nav_msgs::OccupancyGrid::ConstPtr last_global_costmap_msg_;
    ros::Subscriber map_sub_;
    ros::Subscriber tf_static_sub_;
    ros::Subscriber global_costmap_sub_;
    void mapCallback(const nav_msgs::OccupancyGrid::ConstPtr &msg);
    void tfStaticCallback(const tf2_msgs::TFMessage::ConstPtr &msg);
    void globalCostmapCallback(const nav_msgs::OccupancyGrid::ConstPtr &msg);

    int frame_idx_;
};
