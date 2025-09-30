#include "advanced_bag_recorder/advanced_bag_recorder.h"
#include <XmlRpcValue.h>
#include <sys/statvfs.h>
#include <iomanip>
#include <vector>
#include <algorithm>
#include <cstdio>
#include <sstream>

AdvancedBagRecorder::AdvancedBagRecorder(ros::NodeHandle &nh) : nh_(nh), frame_idx_(0)
{
    loadParams();
    manageFolders();
    checkFreeSpaceOrExit();
    openNewBag();

    timer_ = nh_.createTimer(ros::Duration(split_time_), &AdvancedBagRecorder::splitBagCallback, this);

    // 订阅 costmap
    sub_costmap_ = nh_.subscribe(costmap_topic_, 10, &AdvancedBagRecorder::costmapCallback, this);

    // 订阅普通话题
    for (auto &topic : normal_topics_)
    {
        ros::Subscriber sub = nh_.subscribe<topic_tools::ShapeShifter>(
            topic, 10,
            boost::bind(&AdvancedBagRecorder::genericCallback, this, _1, topic));
        subs_.push_back(sub);
    }

    last_costmap_time_ = ros::Time::now();
}

void AdvancedBagRecorder::loadParams()
{
    nh_.param("split_time", split_time_, 60.0);
    nh_.param("max_bags", max_bags_, 60);
    nh_.param("costmap_topic", costmap_topic_, std::string("/decision_planning/mower_mission/obs_costmap"));
    nh_.param("costmap_output_topic", costmap_output_topic_, std::string("/compressed_costmap"));
    nh_.param("costmap_save_rate", costmap_save_rate_, 1.0);
    nh_.param("costmap_jpeg_quality", costmap_jpeg_quality_, 80);
    nh_.param("/advanced_bag_recorder/bag_root_path", bag_root_path_, std::string("/tmp/rosbags"));
    ROS_INFO("bag_root_path: %s", bag_root_path_.c_str());
    nh_.param("max_folders", max_folders_, 3);
    nh_.param("min_free_space_gb", min_free_space_gb_, 2.0);

    XmlRpc::XmlRpcValue normal_topics_xml;
    if (nh_.getParam("normal_topics", normal_topics_xml) && normal_topics_xml.getType() == XmlRpc::XmlRpcValue::TypeArray)
    {
        for (int i = 0; i < normal_topics_xml.size(); ++i)
        {
            normal_topics_.push_back(static_cast<std::string>(normal_topics_xml[i]));
        }
    }

    XmlRpc::XmlRpcValue topic_max_msgs_xml;
    if (nh_.getParam("topic_max_msgs", topic_max_msgs_xml))
    {
        for (auto it = topic_max_msgs_xml.begin(); it != topic_max_msgs_xml.end(); ++it)
        {
            topic_max_msgs_map_[it->first] = static_cast<int>(it->second);
        }
    }
}

void AdvancedBagRecorder::manageFolders()
{
    if (!fs::exists(bag_root_path_))
        fs::create_directories(bag_root_path_);

    std::vector<int> folder_ids;
    for (auto &entry : fs::directory_iterator(bag_root_path_))
    {
        if (fs::is_directory(entry))
        {
            std::string name = entry.path().filename().string();
            try
            {
                folder_ids.push_back(std::stoi(name));
            }
            catch (...)
            {
            }
        }
    }

    std::sort(folder_ids.begin(), folder_ids.end());
    while ((int)folder_ids.size() >= max_folders_)
    {
        int oldest = folder_ids.front();
        fs::remove_all(fs::path(bag_root_path_) / std::to_string(oldest));
        folder_ids.erase(folder_ids.begin());
    }

    // 重编号
    int new_id = 1;
    for (int id : folder_ids)
    {
        if (id != new_id)
            fs::rename(fs::path(bag_root_path_) / std::to_string(id),
                       fs::path(bag_root_path_) / std::to_string(new_id));
        new_id++;
    }

    current_folder_ = (fs::path(bag_root_path_) / std::to_string(new_id)).string();
    fs::create_directories(current_folder_);
    ROS_INFO("Current bag folder: %s", current_folder_.c_str());
}

bool AdvancedBagRecorder::checkFreeSpaceOrExit()
{
    struct statvfs stat;
    if (statvfs(bag_root_path_.c_str(), &stat) != 0)
    {
        ROS_WARN("Failed to get filesystem stats for %s", bag_root_path_.c_str());
        return true;
    }

    double free_bytes = stat.f_bavail * stat.f_frsize;
    double free_gb = free_bytes / (1024.0 * 1024.0 * 1024.0);

    if (free_gb < min_free_space_gb_)
    {
        ROS_ERROR("Disk free space too low: %.2f GB < %.2f GB. Exiting.", free_gb, min_free_space_gb_);
        ros::shutdown();
        return false;
    }
    return true;
}

void AdvancedBagRecorder::splitBagCallback(const ros::TimerEvent &)
{
    if (!checkFreeSpaceOrExit())
        return;

    closeCurrentBag();
    removeOldBagsIfNeeded();
    openNewBag();
}

void AdvancedBagRecorder::genericCallback(const topic_tools::ShapeShifter::ConstPtr &msg, const std::string &topic)
{
    if (!checkFreeSpaceOrExit())
        return;

    int max_msgs = -1;
    if (topic_max_msgs_map_.count(topic))
        max_msgs = topic_max_msgs_map_[topic];
    if (max_msgs > 0 && topic_msg_count_[topic] >= max_msgs)
        return;

    current_bag_.write(topic, ros::Time::now(), msg);
    topic_msg_count_[topic]++;
}

void AdvancedBagRecorder::costmapCallback(const nav_msgs::OccupancyGrid::ConstPtr &msg)
{
    if (msg->info.width == 0 || msg->info.height == 0 || msg->data.empty())
    {
        ROS_WARN("Received empty costmap, skip encoding.");
        return;
    }

    // 转成 OpenCV Mat
    cv::Mat mat(msg->info.height, msg->info.width, CV_8UC1);
    for (size_t i = 0; i < msg->data.size(); ++i)
    {
        int value = msg->data[i];
        if (value == -1)
            value = 127; // 未知区域设为灰色
        else
            value = static_cast<int>(255 - (value * 255 / 100));
        mat.data[i] = static_cast<uchar>(value);
    }

    if (mat.empty())
    {
        ROS_WARN("Costmap cv::Mat is empty, skip.");
        return;
    }

    // 压缩成 JPEG
    std::vector<uchar> buffer;
    std::vector<int> params = {cv::IMWRITE_JPEG_QUALITY, costmap_jpeg_quality_};
    try
    {
        cv::imencode(".jpg", mat, buffer, params);
    }
    catch (cv::Exception &e)
    {
        ROS_ERROR("imencode failed: %s", e.what());
        return;
    }

    // 写入 bag
    rosbag::Bag &bag = current_bag_;
    sensor_msgs::CompressedImage img_msg;
    img_msg.header.stamp = msg->header.stamp;
    img_msg.header.frame_id = msg->header.frame_id;
    img_msg.format = "jpeg";
    img_msg.data = buffer;

    bag.write(costmap_output_topic_, msg->header.stamp, img_msg);
}

void AdvancedBagRecorder::openNewBag()
{
    std::ostringstream ss;
    ss << "bag_" << std::setw(5) << std::setfill('0') << frame_idx_++ << ".bag";
    std::string bag_path = (fs::path(current_folder_) / ss.str()).string();

    current_bag_.open(bag_path, rosbag::bagmode::Write);
    current_bag_.setCompression(rosbag::compression::LZ4); // 设置 LZ4 压缩

    bags_.push_back({bag_path, ros::Time::now()}); // 只保存路径和时间
    topic_msg_count_.clear();
    ROS_INFO("Opened new bag: %s", bag_path.c_str());
}

void AdvancedBagRecorder::closeCurrentBag()
{
    try
    {
        current_bag_.close();
    }
    catch (const rosbag::BagException &e)
    {
        ROS_WARN("Failed to close bag: %s", e.what());
    }
}

void AdvancedBagRecorder::removeOldBagsIfNeeded()
{
    while ((int)bags_.size() > max_bags_)
    {
        BagInfo &old = bags_.front();
        current_bag_.close(); // 保证当前 bag 关闭
        std::remove(old.filename.c_str());
        ROS_INFO("Removed old bag: %s", old.filename.c_str());
        bags_.pop_front();
    }
}

int main(int argc, char **argv)
{
    ros::init(argc, argv, "advanced_bag_recorder_node");
    ros::NodeHandle nh("~");

    AdvancedBagRecorder node(nh);
    ros::spin();

    return 0;
}
