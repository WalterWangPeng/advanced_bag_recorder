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
    ROS_INFO("structor");
    openNewBag();

    timer_ = nh_.createTimer(ros::Duration(split_time_), &AdvancedBagRecorder::splitBagCallback, this);

    // 订阅 costmap
    sub_costmap_ = nh_.subscribe(costmap_topic_, 10, &AdvancedBagRecorder::costmapCallback, this);
    footprint_sub_ = nh_.subscribe<geometry_msgs::PolygonStamped>(
        footprint_topic_, 10, &AdvancedBagRecorder::footprintCallback, this);
    map_sub_ = nh.subscribe(map_topic_, 1, &AdvancedBagRecorder::mapCallback, this);
    tf_static_sub_ = nh.subscribe(tf_static_topic_, 1, &AdvancedBagRecorder::tfStaticCallback, this);
    global_costmap_sub_ = nh.subscribe(global_costmal_topic_, 1, &AdvancedBagRecorder::globalCostmapCallback, this);

    // 订阅普通话题
    for (auto &topic : normal_topics_)
    {
        ros::Subscriber sub = nh_.subscribe<topic_tools::ShapeShifter>(
            topic, 10,
            boost::bind(&AdvancedBagRecorder::genericCallback, this, _1, topic));
        subs_.push_back(sub);
    }

    last_costmap_time_ = ros::Time::now();
    costmap_image_pub_ = nh_.advertise<sensor_msgs::Image>(costmap_output_topic_, 1);
}

void printTopicMaxMsgsMap(const std::unordered_map<std::string, int> &topic_max_msgs_map)
{
    if (topic_max_msgs_map.empty())
    {
        ROS_INFO("topic_max_msgs_map is empty.");
        return;
    }

    ROS_INFO("---- topic_max_msgs_map ----");
    for (const auto &kv : topic_max_msgs_map)
    {
        ROS_INFO("Topic: %s  MaxMsgs: %d", kv.first.c_str(), kv.second);
    }
    ROS_INFO("----------------------------");
}

void AdvancedBagRecorder::loadParams()
{
    nh_.param("split_time", split_time_, 60.0);
    nh_.param("max_bags", max_bags_, 60);
    nh_.param("costmap_topic", costmap_topic_, std::string("/decision_planning/mower_mission/obs_costmap"));
    nh_.param("costmap_output_topic", costmap_output_topic_, std::string("/compressed_costmap"));
    nh_.param("costmap_save_rate", costmap_save_rate_, 1.0);
    nh_.param("footprint_topic", footprint_topic_, std::string("/decision_planning/mower_mission/robot_footprint"));
    nh_.param("bag_root_path", bag_root_path_, std::string("/tmp/rosbags"));
    ROS_INFO("bag_root_path: %s", bag_root_path_.c_str());
    nh_.param("max_folders", max_folders_, 3);
    ROS_INFO("max_folders: %d", max_folders_);
    nh_.param("min_free_space_gb", min_free_space_gb_, 2.0);
    nh_.param("map_topic", map_topic_, std::string("/map"));
    nh_.param("global_costmal_topic", global_costmal_topic_, std::string("/move_base/global_costmap/costmap"));
    nh_.param("tf_static_topic", tf_static_topic_, std::string("/tf_static"));

    XmlRpc::XmlRpcValue normal_topics_xml;
    if (nh_.getParam("normal_topics", normal_topics_xml) && normal_topics_xml.getType() == XmlRpc::XmlRpcValue::TypeArray)
    {
        for (int i = 0; i < normal_topics_xml.size(); ++i)
        {
            normal_topics_.push_back(static_cast<std::string>(normal_topics_xml[i]));
        }
    }
    ROS_INFO("normal_topics_:");
    for (auto &topic : normal_topics_)
    {
        ROS_INFO("%s", topic.c_str());
    }

    XmlRpc::XmlRpcValue topic_max_msgs_xml;
    if (nh_.getParam("topic_max_msgs", topic_max_msgs_xml) && topic_max_msgs_xml.getType() == XmlRpc::XmlRpcValue::TypeArray)
    {
        for (int i = 0; i < topic_max_msgs_xml.size(); ++i)
        {
            std::string key = static_cast<std::string>(topic_max_msgs_xml[i]["key"]);
            int value = static_cast<int>(topic_max_msgs_xml[i]["value"]);
            topic_max_msgs_map_[key] = value;
        }
    }

    ROS_INFO("topic_max_msgs_map_:");
    printTopicMaxMsgsMap(topic_max_msgs_map_);
}

// 获取当前时间字符串，格式 YYYY-MM-DD-HH-MM-SS
std::string AdvancedBagRecorder::getTimeString()
{
    std::time_t t = std::time(nullptr);
    std::tm tm;
    localtime_r(&t, &tm); // 线程安全的 localtime
    char buf[64];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d-%H-%M-%S", &tm);
    return std::string(buf);
}

void AdvancedBagRecorder::manageFolders()
{
    if (!fs::exists(bag_root_path_))
        fs::create_directories(bag_root_path_);

    // 收集子目录及其修改时间
    std::vector<std::pair<std::string, std::time_t>> folders;
    for (auto &entry : fs::directory_iterator(bag_root_path_))
    {
        if (fs::is_directory(entry))
        {
            std::string name = entry.path().filename().string();
            std::time_t t = 0;
            try
            {
                t = fs::last_write_time(entry.path());
            }
            catch (...)
            {
                t = 0;
            }
            folders.emplace_back(name, t);
        }
    }

    // 按时间从旧到新排序
    std::sort(folders.begin(), folders.end(),
              [](const std::pair<std::string, std::time_t> &a, const std::pair<std::string, std::time_t> &b)
              {
                  return a.second < b.second;
              });

    // 如果超过 max_folders，删除最旧的直到满足数量
    while ((int)folders.size() >= max_folders_)
    {
        auto oldest = folders.front();
        try
        {
            fs::remove_all(fs::path(bag_root_path_) / oldest.first);
            ROS_INFO("Removed old folder: %s", oldest.first.c_str());
        }
        catch (const std::exception &e)
        {
            ROS_WARN("Failed to remove old folder %s: %s", oldest.first.c_str(), e.what());
        }
        folders.erase(folders.begin());
    }

    // 使用当前时间作为本次运行创建的文件夹名（格式 YYYY-MM-DD-HH-MM-SS）
    current_time_str_ = getTimeString();
    current_folder_ = (fs::path(bag_root_path_) / current_time_str_).string();
    try
    {
        fs::create_directories(current_folder_);
    }
    catch (const std::exception &e)
    {
        ROS_ERROR("Failed to create folder %s : %s", current_folder_.c_str(), e.what());
        // 如果创建失败，回退到 bag_root_path_
        current_folder_ = bag_root_path_;
    }
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

void AdvancedBagRecorder::footprintCallback(const geometry_msgs::PolygonStamped::ConstPtr &msg)
{
    {
        std::lock_guard<std::mutex> lock(footprint_mutex_);
        latest_footprint_ = *msg; // 拷贝到变量
        has_latest_footprint_ = true;
    }

    // 写入 bag
    if (current_bag_.isOpen())
    {
        if (ros::Time::now().isZero())
        {
            ROS_WARN_THROTTLE(5, "ros::Time::now() is zero, waiting for /clock...");
            return; // 等待时间有效
        }
        current_bag_.write(footprint_topic_, msg->header.stamp.isZero() ? ros::Time::now() : msg->header.stamp, *msg);
    }
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
    if (ros::Time::now().isZero())
    {
        ROS_WARN_THROTTLE(5, "ros::Time::now() is zero, waiting for /clock...");
        return; // 等待时间有效
    }
    current_bag_.write(topic, ros::Time::now(), msg);
    topic_msg_count_[topic]++;
}

cv::Point worldToImage(const geometry_msgs::Point &pt,
                       const nav_msgs::MapMetaData &info)
{
    int img_x = static_cast<int>((pt.x - info.origin.position.x) / info.resolution);
    int img_y = info.height - 1 - static_cast<int>((pt.y - info.origin.position.y) / info.resolution);
    return cv::Point(img_x, img_y);
}

void AdvancedBagRecorder::costmapCallback(const nav_msgs::OccupancyGrid::ConstPtr &msg)
{
    if (msg->info.width == 0 || msg->info.height == 0 || msg->data.empty())
    {
        return;
    }

    double interval = 1.0 / costmap_save_rate_;
    if ((ros::Time::now() - last_costmap_time_).toSec() < interval)
    {
        return; // 距离上次保存时间太短，跳过这次
    }
    last_costmap_time_ = ros::Time::now();

    // 创建彩色 Mat
    cv::Mat mat(msg->info.height, msg->info.width, CV_8UC3);

    for (size_t i = 0; i < msg->data.size(); ++i)
    {
        int value = msg->data[i];
        cv::Vec3b color;

        if (value == 0)
            color = cv::Vec3b(255, 255, 255); // 白色: 无障碍
        else if (value >= 1 && value <= 98)
            color = cv::Vec3b(200, 200, 200); // 浅灰: 膨胀可通行
        else if (value == 99)
            color = cv::Vec3b(100, 100, 100); // 深灰: 膨胀障碍
        else if (value == 100)
            color = cv::Vec3b(0, 0, 0); // 黑色: 障碍
        else if (value == 101)
            color = cv::Vec3b(128, 0, 128); // 紫色: 地图外
        else if (value == 102)
            color = cv::Vec3b(0, 0, 255); // 红色: 禁区
        else
            color = cv::Vec3b(127, 127, 127); // 其他未知灰色

        int y = i / msg->info.width;
        int x = i % msg->info.width;
        mat.at<cv::Vec3b>(y, x) = color;
    }

    cv::flip(mat, mat, 1); // 1 表示水平翻转

    // 获取最新 footprint
    geometry_msgs::PolygonStamped footprint_copy;
    bool use_footprint = false;
    {
        std::lock_guard<std::mutex> lock(footprint_mutex_);
        if (has_latest_footprint_)
        {
            footprint_copy = latest_footprint_; // 拷贝变量
            use_footprint = true;
        }
    }

    if (use_footprint)
    {
        std::vector<cv::Point> pts;
        for (auto &p : footprint_copy.polygon.points)
        {
            int px = static_cast<int>((p.x - msg->info.origin.position.x) / msg->info.resolution);
            int py = static_cast<int>((p.y - msg->info.origin.position.y) / msg->info.resolution);

            // 水平翻转 x
            px = msg->info.width - 1 - px;
            pts.push_back(cv::Point(px, py));
        }

        if (pts.size() == 4)
        {
            // 绘制矩形轮廓
            const cv::Point *pts_array = pts.data();
            int npts = pts.size();
            cv::polylines(mat, &pts_array, &npts, 1, true, cv::Scalar(0, 0, 255), 2); // 红色边框

            // 标记车头：p0-p1 边中点画绿色小圆
            cv::Point head_center = (pts[0] + pts[1]) * 0.5;
            cv::circle(mat, head_center, 3, cv::Scalar(0, 255, 0), -1); // 绿色圆表示车头

            // 可选：标记车尾：p2-p3 边中点画蓝色小圆
            cv::Point tail_center = (pts[2] + pts[3]) * 0.5;
            cv::circle(mat, tail_center, 3, cv::Scalar(255, 0, 0), -1); // 蓝色圆表示车尾
        }
        else if (!pts.empty())
        {
            // 如果点不是四个，也可以画多边形轮廓
            const cv::Point *pts_array = pts.data();
            int npts = pts.size();
            cv::polylines(mat, &pts_array, &npts, 1, true, cv::Scalar(0, 0, 255), 1);
        }
    }

    // 转成 ROS Image 消息
    sensor_msgs::Image img_msg;
    img_msg.header.stamp = msg->header.stamp.isZero() ? ros::Time::now() : msg->header.stamp;
    img_msg.header.frame_id = msg->header.frame_id;
    img_msg.height = mat.rows;
    img_msg.width = mat.cols;
    img_msg.encoding = "bgr8"; // 彩色
    img_msg.is_bigendian = false;
    img_msg.step = mat.cols * 3;
    img_msg.data.assign(mat.data, mat.data + (mat.rows * mat.cols * 3));

    // 写入 bag
    if (ros::Time::now().isZero())
    {
        ROS_WARN_THROTTLE(5, "ros::Time::now() is zero, waiting for /clock...");
        return; // 等待时间有效
    }
    ros::Time stamp = img_msg.header.stamp;
    if (stamp.isZero())
    {
        stamp = ros::Time::now();
    }
    current_bag_.write(costmap_output_topic_, stamp, img_msg);

    // 实时发布
    costmap_image_pub_.publish(img_msg);
}

void AdvancedBagRecorder::openNewBag()
{
    // bag 文件名以 bag 开始写入时的时间命名（YYYY-MM-DD-HH-MM-SS.bag）
    std::string bag_time = getTimeString();
    std::ostringstream ss;
    ss << bag_time << ".bag";
    std::string bag_path = (fs::path(current_folder_) / ss.str()).string();

    try
    {
        current_bag_.open(bag_path, rosbag::bagmode::Write);
        // 有些 ROS 版本支持 setCompression
        try
        {
            current_bag_.setCompression(rosbag::compression::LZ4);
        }
        catch (...)
        {
            // 如果 setCompression 不可用或失败，忽略（仍能写入非压缩 bag）
        }
    }
    catch (const std::exception &e)
    {
        ROS_ERROR("Failed to open bag %s : %s", bag_path.c_str(), e.what());
        return;
    }

    // 记录 bag 信息（只保存路径和时间）
    bags_.push_back({bag_path, ros::Time::now()});
    topic_msg_count_.clear();
    ROS_INFO("Opened new bag: %s", bag_path.c_str());

    // 写入缓存的特殊话题
    if (last_map_msg_)
    {
        current_bag_.write(map_topic_, ros::Time::now(), last_map_msg_);
    }
    if (last_tf_static_msg_)
    {
        current_bag_.write(tf_static_topic_, ros::Time::now(), last_tf_static_msg_);
    }
    if (last_global_costmap_msg_)
    {
        current_bag_.write(global_costmal_topic_, ros::Time::now(), last_global_costmap_msg_);
    }
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

void AdvancedBagRecorder::mapCallback(const nav_msgs::OccupancyGrid::ConstPtr &msg)
{
    last_map_msg_ = msg;
}

void AdvancedBagRecorder::tfStaticCallback(const tf2_msgs::TFMessage::ConstPtr &msg)
{
    last_tf_static_msg_ = msg;
}

void AdvancedBagRecorder::globalCostmapCallback(const nav_msgs::OccupancyGrid::ConstPtr &msg)
{
    last_global_costmap_msg_ = msg;
}
