// Copyright 2015 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include <regex>
#include <iterator>
#include <list>

// include ROS 1
#ifdef __clang__
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wunused-parameter"
#endif

#include "ros/ros.h"

#ifdef __clang__
# pragma clang diagnostic pop
#endif

#include "ros/this_node.h"
#include "ros/header.h"
#include "ros/service_manager.h"
#include "ros/transport/transport_tcp.h"

// include ROS 2
#include "rclcpp/rclcpp.hpp"
#include "rclcpp/scope_exit.hpp"

#include "ros1_bridge/bridge.hpp"


std::mutex g_bridge_mutex;

namespace ros1_bridge {
    std::unique_ptr <ros1_bridge::ServiceFactoryInterface>
    get_service_factory(const std::string &, const std::string &, const std::string &);
}

struct Bridge1to2HandlesAndMessageTypes {
    ros1_bridge::Bridge1to2Handles bridge_handles;
    std::string ros1_type_name;
    std::string ros2_type_name;
};

struct Bridge2to1HandlesAndMessageTypes {
    ros1_bridge::Bridge2to1Handles bridge_handles;
    std::string ros1_type_name;
    std::string ros2_type_name;
};

typedef std::map <std::string, std::vector<std::regex >> WhiteListMap;

bool check_inregex_list(const std::vector <std::regex> &regex_list, const std::string &name,
                        std::set <std::string> &known_set) {
  if (known_set.count(name) != 0) {
    return true;
  }
  for (auto const &rgxp: regex_list) {
    if (std::regex_match(name, rgxp)) {
      known_set.insert(name);
      return true;
    }
  }
  return false;
}

bool find_command_option(const std::vector <std::string> &args, const std::string &option) {
  return std::find(args.begin(), args.end(), option) != args.end();
}

bool get_flag_option(const std::vector <std::string> &args, const std::string &option) {
  auto it = std::find(args.begin(), args.end(), option);
  return it != args.end();
}

std::string
get_flag_val(const std::vector <std::string> &args, const std::string &option, const std::string &default_val) {
  auto it = std::find(args.begin(), args.end(), option);
  if (it == args.end()) {
    return default_val;
  } else {
    auto next = std::next(it);
    if (next == args.end()) {
      return default_val;
    }
    return *next;
  }
}

bool parse_command_options(
        int argc, char **argv, bool &output_topic_introspection,
        bool &bridge_all_1to2_topics, bool &bridge_all_2to1_topics,
        std::string &topic_rgxp_list_param, std::string &srv_rgxp_list_param,
        std::string &node_suffix) {
  std::vector <std::string> args(argv, argv + argc);

  if (find_command_option(args, "-h") || find_command_option(args, "--help")) {
    std::stringstream ss;
    ss << "Usage:" << std::endl;
    ss << " -h, --help: This message." << std::endl;
    ss << " --show-introspection: Print output of introspection of both sides of the bridge.";
    ss << std::endl;
    ss << " --print-pairs: Print a list of the supported ROS 2 <=> ROS 1 conversion pairs.";
    ss << std::endl;
    ss << " --bridge-all-topics: Bridge all topics in both directions, whether or not there is ";
    ss << "a matching subscriber." << std::endl;
    ss << " --bridge-all-1to2-topics: Bridge all ROS 1 topics to ROS 2, whether or not there is ";
    ss << "a matching subscriber." << std::endl;
    ss << " --bridge-all-2to1-topics: Bridge all ROS 2 topics to ROS 1, whether or not there is ";
    ss << "a matching subscriber." << std::endl;
    ss << " --topic-regex-list: ROS1 param holding the list of whitelist topic regex (default: topics_re)";
    ss << std::endl;
    ss << " --service-regex-list: ROS1 param holding the list of whitelist service regex (default: services_re)";
    ss << std::endl;
    ss << " --node-suffix: Suffix used to uniquely identify this node ros12_bridge_<suffix> (default: default)";
    ss << std::endl;
    std::cout << ss.str();
    return false;
  }

  if (get_flag_option(args, "--print-pairs")) {
    auto mappings_2to1 = ros1_bridge::get_all_message_mappings_2to1();
    if (mappings_2to1.size() > 0) {
      printf("Supported ROS 2 <=> ROS 1 message type conversion pairs:\n");
      for (auto &pair : mappings_2to1) {
        printf("  - '%s' (ROS 2) <=> '%s' (ROS 1)\n", pair.first.c_str(), pair.second.c_str());
      }
    } else {
      printf("No message type conversion pairs supported.\n");
    }
    mappings_2to1 = ros1_bridge::get_all_service_mappings_2to1();
    if (mappings_2to1.size() > 0) {
      printf("Supported ROS 2 <=> ROS 1 service type conversion pairs:\n");
      for (auto &pair : mappings_2to1) {
        printf("  - '%s' (ROS 2) <=> '%s' (ROS 1)\n", pair.first.c_str(), pair.second.c_str());
      }
    } else {
      printf("No service type conversion pairs supported.\n");
    }
    return false;
  }

  output_topic_introspection = get_flag_option(args, "--show-introspection");

  bool bridge_all_topics = get_flag_option(args, "--bridge-all-topics");
  bridge_all_1to2_topics = bridge_all_topics || get_flag_option(args, "--bridge-all-1to2-topics");
  bridge_all_2to1_topics = bridge_all_topics || get_flag_option(args, "--bridge-all-2to1-topics");

  topic_rgxp_list_param = get_flag_val(args, "--topic-regex-list", "topics_re");
  srv_rgxp_list_param = get_flag_val(args, "--service-regex-list", "services_re");
  node_suffix = get_flag_val(args, "--node-suffix", "default");

  return true;
}

void update_bridge(
        ros::NodeHandle &ros1_node,
        rclcpp::Node::SharedPtr ros2_node,
        const std::map <std::string, std::string> &ros1_publishers,
        const std::map <std::string, std::string> &ros1_subscribers,
        const std::map <std::string, std::string> &ros2_publishers,
        const std::map <std::string, std::string> &ros2_subscribers,
        const std::map <std::string, std::map<std::string, std::string>> &ros1_services,
        const std::map <std::string, std::map<std::string, std::string>> &ros2_services,
        std::map <std::string, Bridge1to2HandlesAndMessageTypes> &bridges_1to2,
        std::map <std::string, Bridge2to1HandlesAndMessageTypes> &bridges_2to1,
        std::map <std::string, ros1_bridge::ServiceBridge1to2> &service_bridges_1_to_2,
        std::map <std::string, ros1_bridge::ServiceBridge2to1> &service_bridges_2_to_1,
        bool bridge_all_1to2_topics, bool bridge_all_2to1_topics) {
  std::lock_guard <std::mutex> lock(g_bridge_mutex);

  // create 1to2 bridges
  for (const auto& ros1_publisher : ros1_publishers) {
    // identify topics available as ROS 1 publishers as well as ROS 2 subscribers
    const std::string& topic_name = ros1_publisher.first;
    std::string ros1_type_name = ros1_publisher.second;
    std::string ros2_type_name;

    auto ros2_subscriber = ros2_subscribers.find(topic_name);
    if (ros2_subscriber == ros2_subscribers.end()) {
      if (!bridge_all_1to2_topics) {
        continue;
      }
      // update the ROS 2 type name to be that of the anticipated bridged type
      // TODO(dhood): support non 1-1 "bridge-all" mappings
      bool mapping_found = ros1_bridge::get_1to2_mapping(ros1_type_name, ros2_type_name);
      if (!mapping_found) {
        //printf("No known mapping for ROS 1 type '%s'\n", ros1_type_name.c_str());
        continue;
      }
      //printf("topic name '%s' has ROS 2 publishers\n", topic_name.c_str());
    } else {
      ros2_type_name = ros2_subscriber->second;
      // printf("topic name '%s' has ROS 1 publishers and ROS 2 subscribers\n", topic_name.c_str());
    }

    // check if 1to2 bridge for the topic exists
    auto it = bridges_1to2.find(topic_name);
    if (it != bridges_1to2.end()) {
      const auto& bridge = it->second;
      if (bridge.ros1_type_name == ros1_type_name && bridge.ros2_type_name == ros2_type_name) {
        // skip if bridge with correct types is already in place
        continue;
      }
      // remove existing bridge with previous types
      bridges_1to2.erase(it);
      RCUTILS_LOG_INFO("replace 1to2 bridge for topic '%s'\n", topic_name.c_str());
    }

    Bridge1to2HandlesAndMessageTypes bridge;
    bridge.ros1_type_name = ros1_type_name;
    bridge.ros2_type_name = ros2_type_name;

    try {
      bridge.bridge_handles = ros1_bridge::create_bridge_from_1_to_2(
              ros1_node, ros2_node,
              bridge.ros1_type_name, topic_name, 10,
              bridge.ros2_type_name, topic_name, 10);
    } catch (const std::runtime_error &e) {
      RCUTILS_LOG_ERROR(
              "failed to create 1to2 bridge for topic '%s' "
              "with ROS 1 type '%s' and ROS 2 type '%s': %s\n",
              topic_name.c_str(), bridge.ros1_type_name.c_str(), bridge.ros2_type_name.c_str(), e.what());
      if (std::string(e.what()).find("No template specialization") != std::string::npos) {
        RCUTILS_LOG_ERROR("check the list of supported pairs with the `--print-pairs` option\n");
      }
      continue;
    }

    bridges_1to2[topic_name] = bridge;
    RCUTILS_LOG_INFO(
            "created 1to2 bridge for topic '%s' with ROS 1 type '%s' and ROS 2 type '%s'\n",
            topic_name.c_str(), bridge.ros1_type_name.c_str(), bridge.ros2_type_name.c_str());
  }

  // create 2to1 bridges
  for (const auto& ros2_publisher : ros2_publishers) {
    // identify topics available as ROS 1 subscribers as well as ROS 2 publishers
    const std::string& topic_name = ros2_publisher.first;
    std::string ros2_type_name = ros2_publisher.second;
    std::string ros1_type_name;

    auto ros1_subscriber = ros1_subscribers.find(topic_name);
    if (ros1_subscriber == ros1_subscribers.end()) {
      if (!bridge_all_2to1_topics) {
        continue;
      }
      // update the ROS 1 type name to be that of the anticipated bridged type
      // TODO(dhood): support non 1-1 "bridge-all" mappings
      bool mapping_found = ros1_bridge::get_2to1_mapping(ros2_type_name, ros1_type_name);
      if (!mapping_found) {
        // printf("No known mapping for ROS 2 type '%s'\n", ros2_type_name.c_str());
        continue;
      }
      // printf("topic name '%s' has ROS 2 publishers\n", topic_name.c_str());
    } else {
      ros1_type_name = ros1_subscriber->second;
      // printf("topic name '%s' has ROS 1 subscribers and ROS 2 publishers\n", topic_name.c_str());
    }

    // check if 2to1 bridge for the topic exists
    auto it = bridges_2to1.find(topic_name);
    if (it != bridges_2to1.end()) {
      const auto& bridge = it->second;
      if ((bridge.ros1_type_name == ros1_type_name || bridge.ros1_type_name == "") &&
          bridge.ros2_type_name == ros2_type_name) {
        // skip if bridge with correct types is already in place
        continue;
      }
      // remove existing bridge with previous types
      bridges_2to1.erase(it);
      RCUTILS_LOG_INFO("replace 2to1 bridge for topic '%s'\n", topic_name.c_str());
    }

    Bridge2to1HandlesAndMessageTypes bridge;
    bridge.ros1_type_name = ros1_type_name;
    bridge.ros2_type_name = ros2_type_name;

    try {
      bridge.bridge_handles = ros1_bridge::create_bridge_from_2_to_1(
              ros2_node, ros1_node,
              bridge.ros2_type_name, topic_name, 10,
              bridge.ros1_type_name, topic_name, 10);
    } catch (const std::runtime_error &e) {
      RCUTILS_LOG_ERROR(
              "failed to create 2to1 bridge for topic '%s' "
              "with ROS 2 type '%s' and ROS 1 type '%s': %s\n",
              topic_name.c_str(), bridge.ros2_type_name.c_str(), bridge.ros1_type_name.c_str(), e.what());
      if (std::string(e.what()).find("No template specialization") != std::string::npos) {
        RCUTILS_LOG_ERROR("check the list of supported pairs with the `--print-pairs` option\n");
      }
      continue;
    }

    bridges_2to1[topic_name] = bridge;
    RCUTILS_LOG_INFO(
            "created 2to1 bridge for topic '%s' with ROS 2 type '%s' and ROS 1 type '%s'\n",
            topic_name.c_str(), bridge.ros2_type_name.c_str(), bridge.ros1_type_name.c_str());
  }

  // remove obsolete bridges
  //Below appears to cause stability issues on unreliable networks.
  /*
  std::vector <std::string> to_be_removed_1to2;
  for (const auto& it : bridges_1to2) {
    const std::string& topic_name = it.first;
    if (
            ros1_publishers.find(topic_name) == ros1_publishers.end() ||
            (!bridge_all_1to2_topics && ros2_subscribers.find(topic_name) == ros2_subscribers.end())) {
      to_be_removed_1to2.push_back(topic_name);
    }
  }
  for (const auto& topic_name : to_be_removed_1to2) {
    bridges_1to2.erase(topic_name);
    RCUTILS_LOG_INFO("removed 1to2 bridge for topic '%s'\n", topic_name.c_str());
  }
  */
  /*
  std::vector <std::string> to_be_removed_2to1;
  for (const auto& it : bridges_2to1) {
    std::string topic_name = it.first;
    if (
            (!bridge_all_2to1_topics && ros1_subscribers.find(topic_name) == ros1_subscribers.end()) ||
            ros2_publishers.find(topic_name) == ros2_publishers.end()) {
      to_be_removed_2to1.push_back(topic_name);
    }
  }
  for (const auto& topic_name : to_be_removed_2to1) {
    bridges_2to1.erase(topic_name);
    RCUTILS_LOG_INFO("removed 2to1 bridge for topic '%s'\n", topic_name.c_str());
  }
  */

  // create bridges for ros1 services
  for (const auto &service : ros1_services) {
    auto &name = service.first;
    auto &details = service.second;
    if (
            service_bridges_2_to_1.find(name) == service_bridges_2_to_1.end() &&
            service_bridges_1_to_2.find(name) == service_bridges_1_to_2.end()) {
      auto factory = ros1_bridge::get_service_factory(
              "ros1", details.at("package"), details.at("name"));
      if (factory) {
        try {
          service_bridges_2_to_1[name] = factory->service_bridge_2_to_1(ros1_node, ros2_node, name);
          RCUTILS_LOG_INFO("Created 2 to 1 bridge for service %s\n", name.data());
        } catch (const std::runtime_error &e) {
          RCUTILS_LOG_ERROR("Failed to created a bridge: %s\n", e.what());
        }
      } else {
        RCUTILS_LOG_WARN_ONCE_NAMED("dynamic_whitelist_bridge",
                                    "Can't bridge service ROS1=>ROS2 Service: %s for ROS1 Type: %s/%s", name.c_str(),
                                    details.at("package").c_str(), details.at("name").c_str());
      }
    }
  }

  // create bridges for ros2 services
  for (const auto &service : ros2_services) {
    auto &name = service.first;
    auto &details = service.second;
    if (
            service_bridges_1_to_2.find(name) == service_bridges_1_to_2.end() &&
            service_bridges_2_to_1.find(name) == service_bridges_2_to_1.end()) {
      auto factory = ros1_bridge::get_service_factory(
              "ros2", details.at("package"), details.at("name"));
      if (factory) {
        try {
          service_bridges_1_to_2[name] = factory->service_bridge_1_to_2(ros1_node, ros2_node, name);
          RCUTILS_LOG_INFO("Created 1 to 2 bridge for service %s\n", name.data());
        } catch (const std::runtime_error &e) {
          RCUTILS_LOG_ERROR("Failed to created a bridge: %s\n", e.what());
        }
      } else {
        RCUTILS_LOG_WARN_ONCE_NAMED("dynamic_whitelist_bridge",
                                    "Can't bridge service ROS2=>ROS1 Service: %s for ROS2 Type: %s/%s", name.c_str(),
                                    details.at("package").c_str(), details.at("name").c_str());
      }
    }
  }

  // remove obsolete ros1 services
  for (auto it = service_bridges_2_to_1.begin(); it != service_bridges_2_to_1.end();) {
    if (ros1_services.find(it->first) == ros1_services.end()) {
      RCUTILS_LOG_INFO("Removed 2 to 1 bridge for service %s\n", it->first.data());
      try {
        it = service_bridges_2_to_1.erase(it);
      } catch (const std::runtime_error &e) {
        RCUTILS_LOG_ERROR("There was an error while removing 2 to 1 bridge: %s\n", e.what());
      }
    } else {
      ++it;
    }
  }

  // remove obsolete ros2 services
  for (auto it = service_bridges_1_to_2.begin(); it != service_bridges_1_to_2.end();) {
    if (ros2_services.find(it->first) == ros2_services.end()) {
      RCUTILS_LOG_INFO("Removed 1 to 2 bridge for service %s\n", it->first.data());
      try {
        it->second.server.shutdown();
        it = service_bridges_1_to_2.erase(it);
      } catch (const std::runtime_error &e) {
        RCUTILS_LOG_ERROR("There was an error while removing 1 to 2 bridge: %s\n", e.what());
      }
    } else {
      ++it;
    }
  }
}

void get_ros1_service_info(
        const std::string name, std::map <std::string, std::map<std::string, std::string>> &ros1_services) {
  // NOTE(rkozik):
  // I tried to use Connection class but could not make it work
  // auto callback = [](const ros::ConnectionPtr&, const ros::Header&)
  //                 { printf("Callback\n"); return true; };
  // ros::HeaderReceivedFunc f(callback);
  // ros::ConnectionPtr connection(new ros::Connection);
  // connection->initialize(transport, false, ros::HeaderReceivedFunc());
  ros::ServiceManager manager;
  std::string host;
  std::uint32_t port;
  if (!manager.lookupService(name, host, port)) {
    RCUTILS_LOG_ERROR("Failed to look up %s\n", name.data());
    return;
  }
  ros::TransportTCPPtr transport(new ros::TransportTCP(nullptr, ros::TransportTCP::SYNCHRONOUS));
  auto transport_exit = rclcpp::make_scope_exit([transport]() {
      transport->close();
  });
  if (!transport->connect(host, port)) {
    RCUTILS_LOG_ERROR("Failed to connect to %s:%d\n", host.data(), port);
    return;
  }
  ros::M_string header_out;
  header_out["probe"] = "1";
  header_out["md5sum"] = "*";
  header_out["service"] = name;
  header_out["callerid"] = ros::this_node::getName();
  boost::shared_array <uint8_t> buffer;
  uint32_t len;
  ros::Header::write(header_out, buffer, len);
  std::vector <uint8_t> message(len + 4);
  std::memcpy(&message[0], &len, 4);
  std::memcpy(&message[4], buffer.get(), len);
  transport->write(message.data(), message.size());
  uint32_t length;
  auto read = transport->read(reinterpret_cast<uint8_t *>(&length), 4);
  if (read != 4) {
    RCUTILS_LOG_ERROR("Failed to read a response from a service server\n");
    return;
  }
  std::vector <uint8_t> response(length);
  read = transport->read(response.data(), length);
  if (read < 0 || static_cast<uint32_t>(read) != length) {
    RCUTILS_LOG_ERROR("Failed to read a response from a service server\n");
    return;
  }
  std::string key = name;
  ros1_services[key] = std::map<std::string, std::string>();
  ros::Header header_in;
  std::string error;
  auto success = header_in.parse(response.data(), length, error);
  if (!success) {
    RCUTILS_LOG_ERROR("%s\n", error.data());
    return;
  }
  for (std::string field : {"type"}) {
    std::string value;
    auto success = header_in.getValue(field, value);
    if (!success) {
      RCUTILS_LOG_ERROR("Failed to read '%s' from a header for '%s'\n", field.data(), key.c_str());
      ros1_services.erase(key);
      return;
    }
    ros1_services[key][field] = value;
  }
  std::string t = ros1_services[key]["type"];
  ros1_services[key]["package"] = std::string(t.begin(), t.begin() + t.find("/"));
  ros1_services[key]["name"] = std::string(t.begin() + t.find("/") + 1, t.end());
}

int main(int argc, char *argv[]) {
  bool output_topic_introspection;
  bool bridge_all_1to2_topics;
  bool bridge_all_2to1_topics;
  //parameters
  std::string topic_rgxp_list_param;
  std::string srv_rgxp_list_param;
  std::string node_suffix;

  if (!parse_command_options(
          argc, argv, output_topic_introspection, bridge_all_1to2_topics, bridge_all_2to1_topics,
          topic_rgxp_list_param, srv_rgxp_list_param, node_suffix)) {
    return 0;
  }

  // ROS 1 node
  ros::init(argc, argv, "ros12_bridge_" + node_suffix);
  ros::NodeHandle ros1_node;

  // ROS 2 node
  rclcpp::init(argc, argv);
  auto ros2_node = rclcpp::Node::make_shared("ros12_bridge_" + node_suffix);

  // mapping of available topic names to type names
  std::map <std::string, std::string> ros1_publishers;
  std::map <std::string, std::string> ros1_subscribers;
  std::map <std::string, std::string> ros2_publishers;
  std::map <std::string, std::string> ros2_subscribers;
  std::map <std::string, std::map<std::string, std::string>> ros1_services;
  std::map <std::string, std::map<std::string, std::string>> ros2_services;

  std::map <std::string, Bridge1to2HandlesAndMessageTypes> bridges_1to2;
  std::map <std::string, Bridge2to1HandlesAndMessageTypes> bridges_2to1;
  std::map <std::string, ros1_bridge::ServiceBridge1to2> service_bridges_1_to_2;
  std::map <std::string, ros1_bridge::ServiceBridge2to1> service_bridges_2_to_1;

  // params map
  WhiteListMap whitelist_map =
          {
                  {topic_rgxp_list_param, {}},
                  {srv_rgxp_list_param,   {}},

          };
  // resolve set params from param server
  for (auto& element: whitelist_map) {
    XmlRpc::XmlRpcValue rgxps;
    if (
            ros1_node.getParam(element.first, rgxps) &&
            rgxps.getType() == XmlRpc::XmlRpcValue::TypeArray) {
      for (size_t i = 0; i < static_cast<size_t>(rgxps.size()); ++i) {
        std::string rgxp_str = static_cast<std::string>(rgxps[i]);
        element.second.emplace_back(rgxp_str);
      }
    } else {
      RCUTILS_LOG_ERROR("The parameter '%s' either doesn't exist or isn't an array. Ignoring regex list \n",
              element.first.c_str());
    }
  }
  std::set <std::string> already_ignored_ros1_topics;
  std::set <std::string> already_ignored_ros1_services;
  std::set <std::string> valid_ros1_topics;
  std::set <std::string> valid_ros1_services;
  // setup polling of ROS 1 master
  auto ros1_poll = [
          &ros1_node, ros2_node,
          &ros1_publishers, &ros1_subscribers,
          &ros2_publishers, &ros2_subscribers,
          &bridges_1to2, &bridges_2to1,
          &ros1_services, &ros2_services,
          &service_bridges_1_to_2, &service_bridges_2_to_1,
          &output_topic_introspection,
          &bridge_all_1to2_topics, &bridge_all_2to1_topics,
          &topic_rgxp_list_param, &srv_rgxp_list_param,
          &already_ignored_ros1_topics, &already_ignored_ros1_services,
          &whitelist_map, &valid_ros1_topics, &valid_ros1_services
  ](const ros::TimerEvent &) -> void {
      // collect all topics names which have at least one publisher or subscriber beside this bridge
      std::set <std::string> active_publishers;
      std::set <std::string> active_subscribers;

      XmlRpc::XmlRpcValue args, result, payload;
      args[0] = ros::this_node::getName();
      if (!ros::master::execute("getSystemState", args, result, payload, true)) {
        RCUTILS_LOG_ERROR("failed to get system state from ROS 1 master\n");
        return;
      }
      // check publishers
      if (payload.size() >= 1) {
        for (int j = 0; j < payload[0].size(); ++j) {
          const std::string& topic_name = payload[0][j][0];
          for (int k = 0; k < payload[0][j][1].size(); ++k) {
            const std::string& node_name = payload[0][j][1][k];
            // ignore publishers from the bridge itself
            if (node_name == ros::this_node::getName()) {
              continue;
            }
            if (already_ignored_ros1_topics.count(topic_name) == 0) {
              if (check_inregex_list(whitelist_map[topic_rgxp_list_param], topic_name, valid_ros1_topics)) {
                active_publishers.insert(topic_name);
              } else {
                RCUTILS_LOG_INFO(
                        "ignoring topic '%s',as it does not match any regex \n",
                        topic_name.c_str()
                );
                already_ignored_ros1_topics.insert(topic_name);
              }
            }
            break;
          }
        }
      }
      // check subscribers
      if (payload.size() >= 2) {
        for (int j = 0; j < payload[1].size(); ++j) {
          std::string topic_name = payload[1][j][0];
          for (int k = 0; k < payload[1][j][1].size(); ++k) {
            std::string node_name = payload[1][j][1][k];
            // ignore subscribers from the bridge itself
            if (node_name == ros::this_node::getName()) {
              continue;
            }
            if (already_ignored_ros1_topics.count(topic_name) == 0) {
              if (check_inregex_list(whitelist_map[topic_rgxp_list_param], topic_name, valid_ros1_topics)) {
                active_subscribers.insert(topic_name);
              } else {
                RCUTILS_LOG_INFO(
                        "ignoring topic '%s',as it does not match any regex \n",
                        topic_name.c_str()
                );
                already_ignored_ros1_topics.insert(topic_name);
              }
            }
            break;
          }
        }
      }

      // check services
      std::map <std::string, std::map<std::string, std::string>> active_ros1_services;
      if (payload.size() >= 3) {
        for (int j = 0; j < payload[2].size(); ++j) {
          if (payload[2][j][0].getType() == XmlRpc::XmlRpcValue::TypeString) {
            std::string name = payload[2][j][0];
            if (already_ignored_ros1_services.count(name) == 0) {
              if (check_inregex_list(whitelist_map[srv_rgxp_list_param], name, valid_ros1_services)) {
                get_ros1_service_info(name, active_ros1_services);
              } else {
                RCUTILS_LOG_INFO(
                        "ignoring service '%s',as it does not match any regex \n",
                        name.c_str()
                );
                already_ignored_ros1_services.insert(name);
              }
            }
          }
        }
      }
      {
        std::lock_guard <std::mutex> lock(g_bridge_mutex);
        ros1_services = active_ros1_services;
      }

      // get message types for all topics
      ros::master::V_TopicInfo topics;
      bool success = ros::master::getTopics(topics);
      if (!success) {
        RCUTILS_LOG_ERROR("failed to poll ROS 1 master\n");
        return;
      }

      std::map <std::string, std::string> current_ros1_publishers;
      std::map <std::string, std::string> current_ros1_subscribers;
      for (auto topic : topics) {
        auto topic_name = topic.name;
        bool has_publisher = active_publishers.find(topic_name) != active_publishers.end();
        bool has_subscriber = active_subscribers.find(topic_name) != active_subscribers.end();
        if (!has_publisher && !has_subscriber) {
          // skip inactive topics
          continue;
        }
        if (has_publisher) {
          current_ros1_publishers[topic_name] = topic.datatype;
        }
        if (has_subscriber) {
          current_ros1_subscribers[topic_name] = topic.datatype;
        }
        if (output_topic_introspection) {
          RCUTILS_LOG_INFO("ROS 1: %s (%s) [%s pubs, %s subs]\n",
                 topic_name.c_str(), topic.datatype.c_str(),
                 has_publisher ? ">0" : "0", has_subscriber ? ">0" : "0");
        }
      }

      // since ROS 1 subscribers don't report their type they must be added anyway
      for (auto active_subscriber : active_subscribers) {
        if (current_ros1_subscribers.find(active_subscriber) == current_ros1_subscribers.end()) {
          current_ros1_subscribers[active_subscriber] = "";
          if (output_topic_introspection) {
            RCUTILS_LOG_INFO("  ROS 1: %s (<unknown>) sub++\n", active_subscriber.c_str());
          }
        }
      }

      if (output_topic_introspection) {
        RCUTILS_LOG_INFO("\n");
      }

      {
        std::lock_guard <std::mutex> lock(g_bridge_mutex);
        ros1_publishers = current_ros1_publishers;
        ros1_subscribers = current_ros1_subscribers;
      }

      update_bridge(
              ros1_node, ros2_node,
              ros1_publishers, ros1_subscribers,
              ros2_publishers, ros2_subscribers,
              ros1_services, ros2_services,
              bridges_1to2, bridges_2to1,
              service_bridges_1_to_2, service_bridges_2_to_1,
              bridge_all_1to2_topics, bridge_all_2to1_topics);
  };

  auto ros1_poll_timer = ros1_node.createTimer(ros::Duration(1.0), ros1_poll);

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // setup polling of ROS 2
  std::set <std::string> already_ignored_topics;
  std::set <std::string> already_ignored_services;
  std::set <std::string> valid_ros2_topics;
  std::set <std::string> valid_ros2_services;

  auto ros2_poll = [
          &ros1_node, ros2_node,
          &ros1_publishers, &ros1_subscribers,
          &ros2_publishers, &ros2_subscribers,
          &ros1_services, &ros2_services,
          &bridges_1to2, &bridges_2to1,
          &service_bridges_1_to_2, &service_bridges_2_to_1,
          &output_topic_introspection,
          &bridge_all_1to2_topics, &bridge_all_2to1_topics,
          &already_ignored_topics, &already_ignored_services,
          &topic_rgxp_list_param, &srv_rgxp_list_param,
          &whitelist_map, &valid_ros2_topics, &valid_ros2_services
  ]() -> void {
      auto ros2_topics = ros2_node->get_topic_names_and_types();

      std::set <std::string> ignored_topics;
      ignored_topics.insert("parameter_events");

      std::map <std::string, std::string> current_ros2_publishers;
      std::map <std::string, std::string> current_ros2_subscribers;
      for (const auto& topic_and_types : ros2_topics) {
        // ignore some common ROS 2 specific topics
        if (ignored_topics.find(topic_and_types.first) != ignored_topics.end()) {
          continue;
        }

        auto &topic_name = topic_and_types.first;
        auto &topic_type = topic_and_types.second[0];  // explicitly take the first

        // explicitly avoid topics with more than one type
        if (topic_and_types.second.size() > 1) {
          if (already_ignored_topics.count(topic_name) == 0) {
            std::stringstream types;
            for (const auto& type : topic_and_types.second) {
              types << type << ", ";
            }
            RCUTILS_LOG_WARN(
                    "ignoring topic '%s', which has more than one type: [%s]\n",
                    topic_name.c_str(),
                    types.str().substr(0, types.str().length() - 2).c_str()
            );
            already_ignored_topics.insert(topic_name);
          }
          continue;
        }
        if (already_ignored_topics.count(topic_name) == 0) {
          if (!check_inregex_list(whitelist_map[topic_rgxp_list_param], topic_name, valid_ros2_topics)) {
            RCUTILS_LOG_INFO(
                    "ignoring topic '%s',as it does not match any regex \n",
                    topic_name.c_str()
            );
            already_ignored_topics.insert(topic_name);
            continue;
          }
        } else {
          continue;
        }

        auto publisher_count = ros2_node->count_publishers(topic_name);
        auto subscriber_count = ros2_node->count_subscribers(topic_name);

        // ignore publishers from the bridge itself
        if (bridges_1to2.find(topic_name) != bridges_1to2.end()) {
          if (publisher_count > 0) {
            --publisher_count;
          }
        }
        // ignore subscribers from the bridge itself
        if (bridges_2to1.find(topic_name) != bridges_2to1.end()) {
          if (subscriber_count > 0) {
            --subscriber_count;
          }
        }

        if (publisher_count) {
          current_ros2_publishers[topic_name] = topic_type;
        }

        if (subscriber_count) {
          current_ros2_subscribers[topic_name] = topic_type;
        }

        if (output_topic_introspection) {
          RCUTILS_LOG_INFO("  ROS 2: %s (%s) [%ld pubs, %ld subs]\n",
                 topic_name.c_str(), topic_type.c_str(), publisher_count, subscriber_count);
        }
      }

      auto ros2_services_and_types = ros2_node->get_service_names_and_types();
      std::map <std::string, std::map<std::string, std::string>> active_ros2_services;
      for (const auto &service_and_types : ros2_services_and_types) {
        auto &service_name = service_and_types.first;
        auto &service_type = service_and_types.second[0];  // explicitly take the first

        // explicitly avoid services with more than one type
        if (service_and_types.second.size() > 1) {
          if (already_ignored_services.count(service_name) == 0) {
            std::string types = "";
            for (auto type : service_and_types.second) {
              types += type + ", ";
            }
            RCUTILS_LOG_INFO(
                    "ignoring service '%s', which has more than one type: [%s]\n",
                    service_name.c_str(),
                    types.substr(0, types.length() - 2).c_str()
            );
            already_ignored_services.insert(service_name);
          }
          continue;
        }
        if (already_ignored_services.count(service_name) == 0) {
          if (!check_inregex_list(whitelist_map[srv_rgxp_list_param], service_name, valid_ros2_services)) {
            RCUTILS_LOG_INFO(
                    "ignoring topic '%s',as it does not match any regex \n",
                    service_name.c_str()
            );
            already_ignored_services.insert(service_name);
            continue;
          }
        } else {
          continue;
        }

        // TODO(wjwwood): this should be common functionality in the C++ rosidl package
        size_t separator_position = service_type.find('/');
        if (separator_position == std::string::npos) {
          RCUTILS_LOG_ERROR("invalid service type '%s', skipping...\n", service_type.c_str());
          continue;
        }
        auto service_type_package_name = service_type.substr(0, separator_position);
        auto service_type_srv_name = service_type.substr(separator_position + 1);

        // TODO(wjwwood): fix bug where just a ros2 client will cause a ros1 service to be made
        active_ros2_services[service_name]["package"] = service_type_package_name;
        active_ros2_services[service_name]["name"] = service_type_srv_name;
      }

      {
        std::lock_guard <std::mutex> lock(g_bridge_mutex);
        ros2_services = active_ros2_services;
      }

      if (output_topic_introspection) {
        RCUTILS_LOG_INFO("\n");
      }

      {
        std::lock_guard <std::mutex> lock(g_bridge_mutex);
        ros2_publishers = current_ros2_publishers;
        ros2_subscribers = current_ros2_subscribers;
      }

      update_bridge(
              ros1_node, ros2_node,
              ros1_publishers, ros1_subscribers,
              ros2_publishers, ros2_subscribers,
              ros1_services, ros2_services,
              bridges_1to2, bridges_2to1,
              service_bridges_1_to_2, service_bridges_2_to_1,
              bridge_all_1to2_topics, bridge_all_2to1_topics);
  };

  auto ros2_poll_timer = ros2_node->create_wall_timer(
          std::chrono::seconds(1), ros2_poll);


  // ROS 1 asynchronous spinner
  ros::AsyncSpinner async_spinner(1);
  async_spinner.start();

  // ROS 2 spinning loop
  rclcpp::executors::SingleThreadedExecutor executor;
  while (ros1_node.ok() && rclcpp::ok()) {
    executor.spin_node_once(ros2_node);
  }

  return 0;
}
