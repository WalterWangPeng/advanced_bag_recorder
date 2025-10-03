#include "advanced_bag_recorder/advanced_bag_recorder.h"

int main(int argc, char **argv)
{
    ros::init(argc, argv, "advanced_bag_recorder_node");
    ros::NodeHandle nh("~");

    AdvancedBagRecorder node(nh);
    ros::spin();

    return 0;
}
