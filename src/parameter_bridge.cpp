// Copyright 2017 Open Source Robotics Foundation, Inc.
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

#include <xmlrpcpp/XmlRpcException.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <ctime>
#include <cstdarg>
#include <functional>
#include <list>
#include <memory>
#include <signal.h>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

// include ROS 1
#ifdef __clang__
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wunused-parameter"
#endif
#include "ros/ros.h"
#ifdef __clang__
# pragma clang diagnostic pop
#endif

// include ROS 2
#include "rclcpp/rclcpp.hpp"

#include "ros1_bridge/bridge.hpp"
#include "ros1_bridge/factory.hpp"

// Helper function to get current timestamp string
std::string get_timestamp()
{
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
    now.time_since_epoch()) % 1000;

  char buffer[32];
  std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", std::localtime(&time_t_now));

  char result[64];
  snprintf(result, sizeof(result), "%s.%03d", buffer, static_cast<int>(ms.count()));
  return std::string(result);
}

// Simple logging macros - no mutex needed as fprintf+fflush is atomic enough
// and all heavy logging happens during single-threaded initialization
#define LOG_INFO(fmt, ...) \
  do { \
    printf("[%s] [INFO] " fmt "\n", get_timestamp().c_str(), ##__VA_ARGS__); \
    fflush(stdout); \
  } while(0)

#define LOG_WARN(fmt, ...) \
  do { \
    fprintf(stderr, "[%s] [WARN] " fmt "\n", get_timestamp().c_str(), ##__VA_ARGS__); \
    fflush(stderr); \
  } while(0)

#define LOG_ERROR(fmt, ...) \
  do { \
    fprintf(stderr, "[%s] [ERROR] " fmt "\n", get_timestamp().c_str(), ##__VA_ARGS__); \
    fflush(stderr); \
  } while(0)

// ============================================================
// TopicNodeManager: Manages multiple ROS2 nodes for topic sharding
// Creates a new node for every TOPICS_PER_NODE topics to avoid
// DDS/FastDDS performance degradation with too many endpoints per node
// ============================================================
class TopicNodeManager {
public:
  static constexpr size_t TOPICS_PER_NODE = 1000;  // Create new node after this many topics

  TopicNodeManager(const std::string& base_name)
    : base_name_(base_name), topic_count_(0)
  {
    // Create the first topic node
    create_new_node();
  }

  // Get the current node for creating a new topic bridge
  // Automatically creates a new node if current one is full
  rclcpp::Node::SharedPtr get_node_for_topic()
  {
    if (topic_count_ > 0 && (topic_count_ % TOPICS_PER_NODE) == 0) {
      // Current node is full, create a new one
      create_new_node();
    }
    topic_count_++;
    return nodes_.back();
  }

  // Get all nodes (for adding to executors)
  const std::vector<rclcpp::Node::SharedPtr>& get_all_nodes() const
  {
    return nodes_;
  }

  size_t get_node_count() const
  {
    return nodes_.size();
  }

  size_t get_topic_count() const
  {
    return topic_count_;
  }

private:
  void create_new_node()
  {
    std::string node_name;
    if (nodes_.empty()) {
      node_name = base_name_;  // First node uses base name
    } else {
      node_name = base_name_ + "_" + std::to_string(nodes_.size());
    }

    rclcpp::NodeOptions options;
    if (!nodes_.empty()) {
      // Subsequent nodes don't use global arguments to avoid __name:= remapping
      options.use_global_arguments(false);
    }

    auto node = rclcpp::Node::make_shared(node_name, options);
    nodes_.push_back(node);

    LOG_INFO("Created topic node '%s' (node #%zu, for topics %zu-%zu)",
      node_name.c_str(), nodes_.size(),
      (nodes_.size() - 1) * TOPICS_PER_NODE + 1,
      nodes_.size() * TOPICS_PER_NODE);
  }

  std::string base_name_;
  std::vector<rclcpp::Node::SharedPtr> nodes_;
  size_t topic_count_;
};

// ============================================================
// ROS1 Process Partitioning Configuration
// ============================================================
// ROS1 has a single PollManager thread that processes ALL publication queues.
// With 300+ bots publishing at 30Hz, this becomes a bottleneck.
// Solution: Auto-fork child processes, each handling a partition of topics.
//
// Environment variables for manual control:
//   ROS1_BRIDGE_PARTITION_INDEX  - Which partition this process handles (0-based)
//   ROS1_BRIDGE_PARTITION_COUNT  - Total number of partitions
//   ROS1_BRIDGE_NO_AUTO_FORK     - Set to "1" to disable auto-forking
//
// Priority topics (like /clock) always go to partition 0.
// ============================================================

struct PartitionConfig {
  size_t partition_index = 0;
  size_t partition_count = 1;
  bool is_child_process = false;
  std::vector<pid_t> child_pids;

  // Partition thresholds for ROS1 process distribution
  // More partitions = more ROS1 PollManager threads = better throughput
  static constexpr size_t TOPICS_THRESHOLD_HIGH = 2000;
  static constexpr size_t TOPICS_PER_PARTITION_HIGH_LOAD = 500;   // Use when topics > 2000
  static constexpr size_t TOPICS_PER_PARTITION_NORMAL = 1000;     // Use when topics <= 2000

  // Get topics per partition based on total topic count
  static size_t get_topics_per_partition(size_t total_topics) {
    if (total_topics > TOPICS_THRESHOLD_HIGH) {
      return TOPICS_PER_PARTITION_HIGH_LOAD;
    }
    return TOPICS_PER_PARTITION_NORMAL;
  }

  // Check if a topic belongs to this partition
  // Priority topics (like /clock) always go to partition 0
  bool topic_belongs_to_partition(const std::string& topic_name) const {
    if (partition_count <= 1) {
      return true;  // No partitioning
    }

    // Priority topics always go to partition 0
    if (topic_name == "/clock") {
      return partition_index == 0;
    }

    // Hash-based distribution for other topics
    size_t hash_value = std::hash<std::string>{}(topic_name);
    return (hash_value % partition_count) == partition_index;
  }

  // Calculate how many partitions we need based on topic count
  static size_t calculate_partition_count(size_t total_topics) {
    size_t topics_per_partition = get_topics_per_partition(total_topics);
    if (total_topics <= topics_per_partition) {
      return 1;
    }
    return (total_topics + topics_per_partition - 1) / topics_per_partition;
  }
};

// Global partition config (set before forking or from env vars)
static PartitionConfig g_partition_config;

// Signal handler for child process cleanup
static void sigchld_handler(int /*sig*/) {
  int status;
  pid_t pid;
  while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
    // Child process terminated - log and potentially restart
    // For now, just acknowledge
  }
}

// Count total topics from all parameter arrays
size_t count_total_topics(ros::NodeHandle& ros1_node,
                          const char* topics_param,
                          const char* topics_1_to_2_param,
                          const char* topics_2_to_1_param) {
  size_t count = 0;

  XmlRpc::XmlRpcValue topics;
  if (ros1_node.getParam(topics_param, topics) &&
      topics.getType() == XmlRpc::XmlRpcValue::TypeArray) {
    count += static_cast<size_t>(topics.size());
  }

  XmlRpc::XmlRpcValue topics_1_to_2;
  if (ros1_node.getParam(topics_1_to_2_param, topics_1_to_2) &&
      topics_1_to_2.getType() == XmlRpc::XmlRpcValue::TypeArray) {
    count += static_cast<size_t>(topics_1_to_2.size());
  }

  XmlRpc::XmlRpcValue topics_2_to_1;
  if (ros1_node.getParam(topics_2_to_1_param, topics_2_to_1) &&
      topics_2_to_1.getType() == XmlRpc::XmlRpcValue::TypeArray) {
    count += static_cast<size_t>(topics_2_to_1.size());
  }

  return count;
}

rclcpp::QoS qos_from_params(XmlRpc::XmlRpcValue qos_params)
{
  auto ros2_publisher_qos = rclcpp::QoS(rclcpp::KeepLast(10));

  printf("Qos(");

  if (qos_params.getType() == XmlRpc::XmlRpcValue::TypeStruct) {
    if (qos_params.hasMember("history")) {
      auto history = static_cast<std::string>(qos_params["history"]);
      printf("history: ");
      if (history == "keep_all") {
        ros2_publisher_qos.keep_all();
        printf("keep_all, ");
      } else if (history == "keep_last") {
        if (qos_params.hasMember("depth")) {
          auto depth = static_cast<int>(qos_params["depth"]);
          ros2_publisher_qos.keep_last(depth);
          printf("keep_last(%i), ", depth);
        } else {
          fprintf(
            stderr,
            "history: keep_last requires that also a depth is set\n");
        }
      } else {
        fprintf(
          stderr,
          "invalid value for 'history': '%s', allowed values are 'keep_all',"
          "'keep_last' (also requires 'depth' to be set)\n",
          history.c_str());
      }
    }

    if (qos_params.hasMember("reliability")) {
      auto reliability = static_cast<std::string>(qos_params["reliability"]);
      printf("reliability: ");
      if (reliability == "best_effort") {
        ros2_publisher_qos.best_effort();
        printf("best_effort, ");
      } else if (reliability == "reliable") {
        ros2_publisher_qos.reliable();
        printf("reliable, ");
      } else {
        fprintf(
          stderr,
          "invalid value for 'reliability': '%s', allowed values are 'best_effort', 'reliable'\n",
          reliability.c_str());
      }
    }

    if (qos_params.hasMember("durability")) {
      auto durability = static_cast<std::string>(qos_params["durability"]);
      printf("durability: ");
      if (durability == "transient_local") {
        ros2_publisher_qos.transient_local();
        printf("transient_local, ");
      } else if (durability == "volatile") {
        ros2_publisher_qos.durability_volatile();
        printf("volatile, ");
      } else {
        fprintf(
          stderr,
          "invalid value for 'durability': '%s', allowed values are 'best_effort', 'volatile'\n",
          durability.c_str());
      }
    }

    if (qos_params.hasMember("deadline")) {
      try {
        rclcpp::Duration dur = rclcpp::Duration(
          static_cast<int>(qos_params["deadline"]["secs"]),
          static_cast<int>(qos_params["deadline"]["nsecs"]));
        ros2_publisher_qos.deadline(dur);
        printf("deadline: Duration(nsecs: %ld), ", dur.nanoseconds());
      } catch (std::runtime_error & e) {
        fprintf(
          stderr,
          "failed to parse deadline: '%s'\n",
          e.what());
      } catch (XmlRpc::XmlRpcException & e) {
        fprintf(
          stderr,
          "failed to parse deadline: '%s'\n",
          e.getMessage().c_str());
      }
    }

    if (qos_params.hasMember("lifespan")) {
      try {
        rclcpp::Duration dur = rclcpp::Duration(
          static_cast<int>(qos_params["lifespan"]["secs"]),
          static_cast<int>(qos_params["lifespan"]["nsecs"]));
        ros2_publisher_qos.lifespan(dur);
        printf("lifespan: Duration(nsecs: %ld), ", dur.nanoseconds());
      } catch (std::runtime_error & e) {
        fprintf(
          stderr,
          "failed to parse lifespan: '%s'\n",
          e.what());
      } catch (XmlRpc::XmlRpcException & e) {
        fprintf(
          stderr,
          "failed to parse lifespan: '%s'\n",
          e.getMessage().c_str());
      }
    }

    if (qos_params.hasMember("liveliness")) {
      if (qos_params["liveliness"].getType() == XmlRpc::XmlRpcValue::TypeInt) {
        try {
          auto liveliness = static_cast<int>(qos_params["liveliness"]);
          ros2_publisher_qos.liveliness(static_cast<rmw_qos_liveliness_policy_t>(liveliness));
          printf("liveliness: %i, ", static_cast<int>(liveliness));
        } catch (std::runtime_error & e) {
          fprintf(
            stderr,
            "failed to parse liveliness: '%s'\n",
            e.what());
        } catch (XmlRpc::XmlRpcException & e) {
          fprintf(
            stderr,
            "failed to parse liveliness: '%s'\n",
            e.getMessage().c_str());
        }
      } else if (qos_params["liveliness"].getType() == XmlRpc::XmlRpcValue::TypeString) {
        try {
          rmw_qos_liveliness_policy_t liveliness =
            rmw_qos_liveliness_policy_t::RMW_QOS_POLICY_LIVELINESS_SYSTEM_DEFAULT;
          auto liveliness_str = static_cast<std::string>(qos_params["liveliness"]);
          if (liveliness_str == "LIVELINESS_SYSTEM_DEFAULT" ||
            liveliness_str == "liveliness_system_default")
          {
            liveliness = rmw_qos_liveliness_policy_t::RMW_QOS_POLICY_LIVELINESS_SYSTEM_DEFAULT;
          } else if (liveliness_str == "LIVELINESS_AUTOMATIC" ||  // NOLINT
            liveliness_str == "liveliness_automatic")
          {
            liveliness = rmw_qos_liveliness_policy_t::RMW_QOS_POLICY_LIVELINESS_AUTOMATIC;
          } else if (liveliness_str == "LIVELINESS_MANUAL_BY_TOPIC" ||  // NOLINT
            liveliness_str == "liveliness_manual_by_topic")
          {
            liveliness = rmw_qos_liveliness_policy_t::RMW_QOS_POLICY_LIVELINESS_MANUAL_BY_TOPIC;
          } else {
            fprintf(
              stderr,
              "invalid value for 'liveliness': '%s', allowed values are "
              "LIVELINESS_{SYSTEM_DEFAULT, AUTOMATIC, MANUAL_BY_TOPIC}, upper or lower case\n",
              liveliness_str.c_str());
          }

          ros2_publisher_qos.liveliness(liveliness);
          printf("liveliness: %s, ", liveliness_str.c_str());
        } catch (std::runtime_error & e) {
          fprintf(
            stderr,
            "failed to parse liveliness: '%s'\n",
            e.what());
        } catch (XmlRpc::XmlRpcException & e) {
          fprintf(
            stderr,
            "failed to parse liveliness: '%s'\n",
            e.getMessage().c_str());
        }
      } else {
        fprintf(
          stderr,
          "failed to parse liveliness, parameter was not a string or int \n");
      }
    }

    if (qos_params.hasMember("liveliness_lease_duration")) {
      try {
        rclcpp::Duration dur = rclcpp::Duration(
          static_cast<int>(qos_params["liveliness_lease_duration"]["secs"]),
          static_cast<int>(qos_params["liveliness_lease_duration"]["nsecs"]));
        ros2_publisher_qos.liveliness_lease_duration(dur);
        printf("liveliness_lease_duration: Duration(nsecs: %ld), ", dur.nanoseconds());
      } catch (std::runtime_error & e) {
        fprintf(
          stderr,
          "failed to parse liveliness_lease_duration: '%s'\n",
          e.what());
      } catch (XmlRpc::XmlRpcException & e) {
        fprintf(
          stderr,
          "failed to parse liveliness_lease_duration: '%s'\n",
          e.getMessage().c_str());
      }
    }
  } else {
    fprintf(
      stderr,
      "QoS parameters could not be read\n");
  }

  printf(")");
  return ros2_publisher_qos;
}

int main(int argc, char * argv[])
{
  try {
  // Parse command line arguments FIRST (before any ROS init)
  // Argument order (backward compatible):
  //   argv[1]: topics (bidirectional)
  //   argv[2]: services_1_to_2
  //   argv[3]: services_2_to_1
  //   argv[4]: topics_1_to_2 (optional, new)
  //   argv[5]: topics_2_to_1 (optional, new)
  //
  // Filter out ROS-specific arguments (starting with "__" like __name:=, __ns:=, __log:=)
  // These are remapping arguments handled by ROS init, not parameter names.
  std::vector<std::string> filtered_args;
  filtered_args.push_back(argv[0]);  // Keep program name
  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);
    // Skip ROS remapping arguments (start with __ or contain :=__ patterns)
    if (arg.rfind("__", 0) == 0 || arg.find(":=__") != std::string::npos) {
      continue;  // Skip ROS-specific arguments
    }
    // Skip arguments starting with - (ROS options like --ros-args)
    if (arg.rfind("-", 0) == 0) {
      continue;
    }
    filtered_args.push_back(arg);
  }

  const char * topics_parameter_name = "topics";
  const char * services_1_to_2_parameter_name = "services_1_to_2";
  const char * services_2_to_1_parameter_name = "services_2_to_1";
  const char * topics_1_to_2_parameter_name = "topics_1_to_2";
  const char * topics_2_to_1_parameter_name = "topics_2_to_1";

  if (filtered_args.size() > 1) {
    topics_parameter_name = filtered_args[1].c_str();
  }
  if (filtered_args.size() > 2) {
    services_1_to_2_parameter_name = filtered_args[2].c_str();
  }
  if (filtered_args.size() > 3) {
    services_2_to_1_parameter_name = filtered_args[3].c_str();
  }
  if (filtered_args.size() > 4) {
    topics_1_to_2_parameter_name = filtered_args[4].c_str();
  }
  if (filtered_args.size() > 5) {
    topics_2_to_1_parameter_name = filtered_args[5].c_str();
  }

  // ============================================================
  // ROS1 PROCESS PARTITIONING - Auto-fork for scalability
  // ============================================================
  // Check environment variables for partition configuration
  // This allows manual control or is set automatically by parent process
  //
  // Defaults (if no env vars set):
  //   - partition_index = 0
  //   - partition_count = 1 (single process, no partitioning)
  //   - Auto-fork enabled: will fork if topics > 1000
  //
  const char* env_partition_index = std::getenv("ROS1_BRIDGE_PARTITION_INDEX");
  const char* env_partition_count = std::getenv("ROS1_BRIDGE_PARTITION_COUNT");
  const char* env_no_auto_fork = std::getenv("ROS1_BRIDGE_NO_AUTO_FORK");

  // Log environment variable status
  LOG_INFO("ROS1 Partition Config - Environment check:");
  LOG_INFO("  ROS1_BRIDGE_PARTITION_INDEX: %s", env_partition_index ? env_partition_index : "(not set, default: 0)");
  LOG_INFO("  ROS1_BRIDGE_PARTITION_COUNT: %s", env_partition_count ? env_partition_count : "(not set, default: auto)");
  LOG_INFO("  ROS1_BRIDGE_NO_AUTO_FORK: %s", env_no_auto_fork ? env_no_auto_fork : "(not set, default: auto-fork enabled)");

  if (env_partition_index && env_partition_count) {
    // Manual partitioning via environment variables (set by parent's fork+exec)
    g_partition_config.partition_index = static_cast<size_t>(std::atoi(env_partition_index));
    g_partition_config.partition_count = static_cast<size_t>(std::atoi(env_partition_count));
    g_partition_config.is_child_process = (g_partition_config.partition_index > 0);

    LOG_INFO("ROS1 Process Partitioning: partition %zu of %zu (from environment, PID: %d)",
      g_partition_config.partition_index, g_partition_config.partition_count, getpid());
  }

  // ROS 1 node initialization
  // For child partitions, use a partition-specific node name and filter out __name:=
  std::string ros1_node_name = "ros_bridge";
  std::vector<char*> ros1_argv;

  if (g_partition_config.is_child_process) {
    ros1_node_name = "ros_bridge_p" + std::to_string(g_partition_config.partition_index);
    // Filter out __name:= to use our partition-specific name
    for (int i = 0; i < argc; ++i) {
      std::string arg(argv[i]);
      if (arg.find("__name:=") == std::string::npos) {
        ros1_argv.push_back(argv[i]);
      }
    }
  } else {
    // Parent process - use original argv
    for (int i = 0; i < argc; ++i) {
      ros1_argv.push_back(argv[i]);
    }
  }

  int ros1_argc = static_cast<int>(ros1_argv.size());
  ros::init(ros1_argc, ros1_argv.data(), ros1_node_name, ros::init_options::NoSigintHandler);
  ros::NodeHandle ros1_node;

  LOG_INFO("ROS1 initialized as '%s'", ros::this_node::getName().c_str());

  // Count total topics to determine if we need to fork
  size_t total_topics = count_total_topics(ros1_node,
    topics_parameter_name, topics_1_to_2_parameter_name, topics_2_to_1_parameter_name);

  LOG_INFO("Total topics configured: %zu", total_topics);

  // Auto-fork if needed (only if not already partitioned and auto-fork not disabled)
  size_t topics_per_partition = PartitionConfig::get_topics_per_partition(total_topics);
  bool should_auto_fork = !env_partition_index && !env_no_auto_fork &&
    (total_topics > topics_per_partition);

  if (env_no_auto_fork) {
    LOG_INFO("Auto-fork DISABLED via ROS1_BRIDGE_NO_AUTO_FORK environment variable");
  } else if (env_partition_index) {
    LOG_INFO("Auto-fork SKIPPED: using manual partition configuration");
  } else if (total_topics <= topics_per_partition) {
    LOG_INFO("Auto-fork NOT NEEDED: %zu topics <= %zu threshold (single process mode)",
      total_topics, topics_per_partition);
  }

  size_t needed_partitions = 1;
  if (should_auto_fork) {
    needed_partitions = PartitionConfig::calculate_partition_count(total_topics);
    LOG_INFO("Auto-forking: %zu topics requires %zu partitions (%zu topics/partition, high-load mode: %s)",
      total_topics, needed_partitions, topics_per_partition,
      (total_topics > PartitionConfig::TOPICS_THRESHOLD_HIGH) ? "yes" : "no");

    // Set up signal handler for child process cleanup
    struct sigaction sa;
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, nullptr);

    // Flush before fork to ensure clean output separation
    fflush(stdout);
    fflush(stderr);

    // Fork child processes for partitions 1 to N-1
    // Parent (this process) handles partition 0 and KEEPS its ROS1 init
    // Children will init their own ROS1 after fork
    for (size_t i = 1; i < needed_partitions; ++i) {
      pid_t pid = fork();

      if (pid < 0) {
        LOG_ERROR("Failed to fork child process for partition %zu: %s", i, strerror(errno));
        continue;
      } else if (pid == 0) {
        // ============ CHILD PROCESS ============
        // Use exec() to replace this process with a fresh instance.
        // This avoids inheriting corrupted ROS1 state from parent.
        // The new process will see ROS1_BRIDGE_PARTITION_INDEX env var and
        // initialize as a child partition without forking again.

        char partition_index_str[16];
        char partition_count_str[16];
        snprintf(partition_index_str, sizeof(partition_index_str), "%zu", i);
        snprintf(partition_count_str, sizeof(partition_count_str), "%zu", needed_partitions);

        setenv("ROS1_BRIDGE_PARTITION_INDEX", partition_index_str, 1);
        setenv("ROS1_BRIDGE_PARTITION_COUNT", partition_count_str, 1);

        LOG_INFO("Child process %zu: exec'ing fresh instance with PARTITION_INDEX=%s, PARTITION_COUNT=%s",
          i, partition_index_str, partition_count_str);
        fflush(stdout);
        fflush(stderr);

        // Re-exec ourselves with the same arguments
        // This gives us a completely fresh process with no inherited ROS1 state
        execv(argv[0], argv);

        // If exec fails, log and exit
        LOG_ERROR("Child process %zu: execv failed: %s", i, strerror(errno));
        _exit(1);
      } else {
        // ============ PARENT PROCESS ============
        g_partition_config.child_pids.push_back(pid);
        LOG_INFO("Forked child process for partition %zu (PID: %d)", i, pid);
      }
    }

    // Parent process handles partition 0 - keeps its original ROS1 init
    if (!g_partition_config.is_child_process) {
      g_partition_config.partition_index = 0;
      g_partition_config.partition_count = needed_partitions;
      LOG_INFO("Parent process handling partition 0 of %zu (PID: %d), ROS1 node: '%s'",
        needed_partitions, getpid(), ros::this_node::getName().c_str());
    }
  }

  // ROS 2 initialization (after fork to avoid sharing DDS state)
  // For child processes, filter out __name:= to avoid conflicts
  std::string base_name;
  if (g_partition_config.is_child_process) {
    // Filter out __name:= for child processes
    std::vector<char*> child_argv;
    for (int arg_i = 0; arg_i < argc; ++arg_i) {
      std::string arg(argv[arg_i]);
      if (arg.find("__name:=") == std::string::npos) {
        child_argv.push_back(argv[arg_i]);
      }
    }
    int child_argc = static_cast<int>(child_argv.size());
    rclcpp::init(child_argc, child_argv.data());

    // Use partition-specific base name
    base_name = "ros12_bridge_p" + std::to_string(g_partition_config.partition_index);
    LOG_INFO("Child process ROS2 initialized with base name '%s'", base_name.c_str());
  } else {
    rclcpp::init(argc, argv);

    // Create a temporary node just to get the remapped name from __name:=
    auto temp_node = rclcpp::Node::make_shared("ros_bridge");
    base_name = temp_node->get_name();
    temp_node.reset();  // Destroy temp node
  }

  // TopicNodeManager: automatically creates new nodes every 1000 topics
  // This avoids DDS performance degradation with too many endpoints per node
  TopicNodeManager topic_node_manager(base_name);

  // Service node - dedicated node for services (isolated waitset)
  // Only partition 0 handles services
  std::string service_node_name = base_name + "_services";
  rclcpp::NodeOptions service_node_options;
  service_node_options.use_global_arguments(false);
  rclcpp::Node::SharedPtr ros2_service_node = nullptr;

  if (g_partition_config.partition_index == 0) {
    ros2_service_node = rclcpp::Node::make_shared(service_node_name, service_node_options);
    LOG_INFO("Created dedicated ROS2 service node '%s' with isolated waitset", service_node_name.c_str());
  }

  LOG_INFO("Topic node sharding enabled: new node created every %zu topics", TopicNodeManager::TOPICS_PER_NODE);
  if (g_partition_config.partition_count > 1) {
    LOG_INFO("ROS1 Process Partitioning ACTIVE: handling partition %zu of %zu",
      g_partition_config.partition_index, g_partition_config.partition_count);
  }

  std::list<ros1_bridge::BridgeHandles> all_handles;
  std::list<ros1_bridge::ServiceBridge1to2> service_bridges_1_to_2;
  std::list<ros1_bridge::ServiceBridge2to1> service_bridges_2_to_1;

  // ============================================================
  // INITIALIZATION ORDER: Clock -> Services -> Other Topics
  // Clock is the MOST CRITICAL topic - all timing depends on it.
  // Initialize it FIRST so simulation time is available immediately.
  // Services are initialized next so bots can spawn once clock is running.
  // ============================================================

  // -------------------- CLOCK FIRST (ALL PARTITIONS) --------------------
  // Pre-fetch topic parameters for clock priority processing
  // Clock must be initialized before ANYTHING else including services
  XmlRpc::XmlRpcValue topics;
  bool has_topics = ros1_node.getParam(topics_parameter_name, topics) &&
    topics.getType() == XmlRpc::XmlRpcValue::TypeArray;

  XmlRpc::XmlRpcValue topics_2_to_1;
  bool has_topics_2_to_1 = ros1_node.getParam(topics_2_to_1_parameter_name, topics_2_to_1) &&
    topics_2_to_1.getType() == XmlRpc::XmlRpcValue::TypeArray;

  // Helper lambda to create 2_to_1 bridge for clock (defined early for clock initialization)
  // This is a simplified version specifically for /clock initialization
  auto create_clock_bridge_2_to_1 = [&](XmlRpc::XmlRpcValue& topic_entry) -> bool {
    std::string topic_name = static_cast<std::string>(topic_entry["topic"]);
    std::string type_name = static_cast<std::string>(topic_entry["type"]);
    size_t queue_size = static_cast<int>(topic_entry["queue_size"]);
    if (!queue_size) {
      queue_size = 100;
    }

    // Get node for this topic
    auto ros2_node = topic_node_manager.get_node_for_topic();

    LOG_INFO("[P%zu] Creating 2_to_1 bridge for topic '%s' with ROS 2 type '%s' on node '%s'",
      g_partition_config.partition_index, topic_name.c_str(), type_name.c_str(), ros2_node->get_name());

    try {
      ros1_bridge::BridgeHandles handles;
      handles.bridge2to1 = ros1_bridge::create_bridge_from_2_to_1(
        ros2_node, ros1_node,
        type_name, topic_name, queue_size, "", topic_name, queue_size);
      all_handles.push_back(handles);
      return true;
    } catch (std::runtime_error & e) {
      LOG_ERROR("failed to create 2_to_1 bridge for topic '%s': %s", topic_name.c_str(), e.what());
      return false;
    }
  };

  // Helper lambda to create bidirectional bridge for clock
  auto create_clock_bridge_bidir = [&](XmlRpc::XmlRpcValue& topic_entry) -> bool {
    std::string topic_name = static_cast<std::string>(topic_entry["topic"]);
    std::string type_name = static_cast<std::string>(topic_entry["type"]);
    size_t queue_size = static_cast<int>(topic_entry["queue_size"]);
    if (!queue_size) {
      queue_size = 100;
    }

    // Get node for this topic
    auto ros2_node = topic_node_manager.get_node_for_topic();

    LOG_INFO("[P%zu] Creating bidirectional bridge for topic '%s' with ROS 2 type '%s' on node '%s'",
      g_partition_config.partition_index, topic_name.c_str(), type_name.c_str(), ros2_node->get_name());

    try {
      ros1_bridge::BridgeHandles handles = ros1_bridge::create_bidirectional_bridge(
        ros1_node, ros2_node, "", type_name, topic_name, queue_size);
      all_handles.push_back(handles);
      return true;
    } catch (std::runtime_error & e) {
      LOG_ERROR("failed to create bidirectional bridge for topic '%s': %s", topic_name.c_str(), e.what());
      return false;
    }
  };

  // ==================== CLOCK PRIORITY (BEFORE SERVICES) ====================
  // Only partition 0 handles /clock to avoid duplicate publishers
  bool clock_initialized = false;
  if (g_partition_config.partition_index == 0) {
    LOG_INFO("========================================");
    LOG_INFO("Initializing /clock FIRST (highest priority)...");
    LOG_INFO("========================================");

    // /clock from topics_2_to_1 (preferred - unidirectional from ROS2)
    if (has_topics_2_to_1) {
      for (size_t i = 0; i < static_cast<size_t>(topics_2_to_1.size()); ++i) {
        std::string topic_name = static_cast<std::string>(topics_2_to_1[i]["topic"]);
        if (topic_name == "/clock") {
          LOG_INFO("Setting up /clock from topics_2_to_1 (ROS2 -> ROS1)...");
          if (create_clock_bridge_2_to_1(topics_2_to_1[i])) {
            LOG_INFO("/clock bridge initialized (2_to_1). Simulation time is now available.");
            clock_initialized = true;
          }
          break;
        }
      }
    }

    // /clock from bidirectional topics (fallback if not in topics_2_to_1)
    if (!clock_initialized && has_topics) {
      for (size_t i = 0; i < static_cast<size_t>(topics.size()); ++i) {
        std::string topic_name = static_cast<std::string>(topics[i]["topic"]);
        if (topic_name == "/clock") {
          LOG_INFO("Setting up /clock from topics (bidirectional)...");
          if (create_clock_bridge_bidir(topics[i])) {
            LOG_INFO("/clock bridge initialized (bidirectional). Simulation time is now available.");
            clock_initialized = true;
          }
          break;
        }
      }
    }

    if (!clock_initialized) {
      LOG_WARN("/clock topic not found in configuration. Simulation time may not be available!");
    }
  }

  // -------------------- SERVICES SECOND (partition 0 only) --------------------
  if (g_partition_config.partition_index == 0) {
  LOG_INFO("========================================");
  LOG_INFO("Initializing services...");
  LOG_INFO("========================================");
  // ROS 1 Services in ROS 2
  XmlRpc::XmlRpcValue services_1_to_2;
  if (
    ros1_node.getParam(services_1_to_2_parameter_name, services_1_to_2) &&
    services_1_to_2.getType() == XmlRpc::XmlRpcValue::TypeArray)
  {
    for (size_t i = 0; i < static_cast<size_t>(services_1_to_2.size()); ++i) {
      std::string service_name = static_cast<std::string>(services_1_to_2[i]["service"]);
      std::string type_name = static_cast<std::string>(services_1_to_2[i]["type"]);
      {
        // for backward compatibility
        std::string package_name = static_cast<std::string>(services_1_to_2[i]["package"]);
        if (!package_name.empty()) {
          LOG_WARN("The service '%s' uses the key 'package' which is deprecated for "
            "services. Instead prepend the 'type' value with '<package>/'.",
            service_name.c_str());
          type_name = package_name + "/" + type_name;
        }
      }
      LOG_INFO("Trying to create bridge for ROS 2 service '%s' with type '%s'",
        service_name.c_str(), type_name.c_str());

      const size_t index = type_name.find("/");
      if (index == std::string::npos) {
        LOG_ERROR("the service '%s' has a type '%s' without a slash.",
          service_name.c_str(), type_name.c_str());
        continue;
      }
      auto factory = ros1_bridge::get_service_factory(
        "ros2", type_name.substr(0, index), type_name.substr(index + 1));
      if (factory) {
        try {
          service_bridges_1_to_2.push_back(
            factory->service_bridge_1_to_2(
              ros1_node, ros2_service_node, service_name, type_name));
          LOG_INFO("Created 1 to 2 bridge for service %s", service_name.c_str());
        } catch (std::runtime_error & e) {
          LOG_ERROR("failed to create bridge ROS 1 service '%s' with type '%s': %s",
            service_name.c_str(), type_name.c_str(), e.what());
        }
      } else {
        LOG_ERROR("failed to create bridge ROS 1 service '%s' no conversion for type '%s'",
          service_name.c_str(), type_name.c_str());
      }
    }
  } else {
    LOG_WARN("The parameter '%s' either doesn't exist or isn't an array",
      services_1_to_2_parameter_name);
  }

  // ROS 2 Services in ROS 1
  XmlRpc::XmlRpcValue services_2_to_1;
  if (
    ros1_node.getParam(services_2_to_1_parameter_name, services_2_to_1) &&
    services_2_to_1.getType() == XmlRpc::XmlRpcValue::TypeArray)
  {
    for (size_t i = 0; i < static_cast<size_t>(services_2_to_1.size()); ++i) {
      std::string service_name = static_cast<std::string>(services_2_to_1[i]["service"]);
      std::string type_name = static_cast<std::string>(services_2_to_1[i]["type"]);
      {
        // for backward compatibility
        std::string package_name = static_cast<std::string>(services_2_to_1[i]["package"]);
        if (!package_name.empty()) {
          LOG_WARN("The service '%s' uses the key 'package' which is deprecated for "
            "services. Instead prepend the 'type' value with '<package>/'.",
            service_name.c_str());
          type_name = package_name + "/" + type_name;
        }
      }
      LOG_INFO("Trying to create bridge for ROS 1 service '%s' with type '%s'",
        service_name.c_str(), type_name.c_str());

      const size_t index = type_name.find("/");
      if (index == std::string::npos) {
        LOG_ERROR("the service '%s' has a type '%s' without a slash.",
          service_name.c_str(), type_name.c_str());
        continue;
      }

      auto factory = ros1_bridge::get_service_factory(
        "ros1", type_name.substr(0, index), type_name.substr(index + 1));
      if (factory) {
        try {
          service_bridges_2_to_1.push_back(
            factory->service_bridge_2_to_1(ros1_node, ros2_service_node, service_name));
          LOG_INFO("Created 2 to 1 bridge for service %s", service_name.c_str());
        } catch (std::runtime_error & e) {
          LOG_ERROR("failed to create bridge ROS 2 service '%s' with type '%s': %s",
            service_name.c_str(), type_name.c_str(), e.what());
        }
      } else {
        LOG_ERROR("failed to create bridge ROS 2 service '%s' no conversion for type '%s'",
          service_name.c_str(), type_name.c_str());
      }
    }
  } else {
    LOG_WARN("The parameter '%s' either doesn't exist or isn't an array",
      services_2_to_1_parameter_name);
  }

  LOG_INFO("Services initialized. Now setting up remaining topics...");
  }  // End of partition 0 services block

  // -------------------- REMAINING TOPICS --------------------
  // NOTE: /clock is already initialized above (highest priority)
  //
  // PERFORMANCE NOTE: ROS1 has a single PollManager thread that processes ALL
  // publication queues (ros::Publication::processPublishQueue). With 300+ bots
  // publishing high-frequency topics (model_state @ 30Hz), this becomes a bottleneck.
  //
  // Mitigations applied:
  // 1. Smaller queue sizes (10 vs 100) - reduces backlog processed per iteration
  // 2. Topic sharding on ROS2 side - parallel subscription processing
  // 3. ROS1 process partitioning - auto-fork when topics > 1000

  // Helper lambda to create a bidirectional bridge for a topic
  // Uses topic_node_manager to automatically shard across multiple nodes
  // Respects partition assignment for ROS1 process partitioning
  auto create_topic_bridge = [&](XmlRpc::XmlRpcValue& topic_entry) -> bool {
    std::string topic_name = static_cast<std::string>(topic_entry["topic"]);

    // Check if this topic belongs to our partition
    if (!g_partition_config.topic_belongs_to_partition(topic_name)) {
      return false;  // Skip - handled by another partition
    }

    std::string type_name = static_cast<std::string>(topic_entry["type"]);
    size_t queue_size = static_cast<int>(topic_entry["queue_size"]);
    if (!queue_size) {
      queue_size = 100;
    }

    // Get node for this topic (may create a new node if current is full)
    auto ros2_node = topic_node_manager.get_node_for_topic();

    LOG_INFO("[P%zu] Creating bidirectional bridge for topic '%s' with ROS 2 type '%s' on node '%s'",
      g_partition_config.partition_index, topic_name.c_str(), type_name.c_str(), ros2_node->get_name());

    try {
      if (topic_entry.hasMember("qos")) {
        LOG_INFO("Setting up QoS for '%s'", topic_name.c_str());
        auto qos_settings = qos_from_params(topic_entry["qos"]);
        ros1_bridge::BridgeHandles handles = ros1_bridge::create_bidirectional_bridge(
          ros1_node, ros2_node, "", type_name, topic_name, queue_size, qos_settings);
        all_handles.push_back(handles);
      } else {
        ros1_bridge::BridgeHandles handles = ros1_bridge::create_bidirectional_bridge(
          ros1_node, ros2_node, "", type_name, topic_name, queue_size);
        all_handles.push_back(handles);
      }
      return true;
    } catch (std::runtime_error & e) {
      LOG_ERROR("failed to create bidirectional bridge for topic '%s' with ROS 2 type '%s': %s",
        topic_name.c_str(), type_name.c_str(), e.what());
      return false;
    }
  };

  // Helper lambda to create 2_to_1 bridge
  // Uses topic_node_manager to automatically shard across multiple nodes
  // NOTE: For 2_to_1 bridges, we use smaller ROS1 publisher queue sizes to reduce
  // backpressure on the PollManager thread (which processes ALL publish queues)
  // Respects partition assignment for ROS1 process partitioning
  auto create_2_to_1_bridge = [&](XmlRpc::XmlRpcValue& topic_entry) -> bool {
    std::string topic_name = static_cast<std::string>(topic_entry["topic"]);

    // Check if this topic belongs to our partition
    if (!g_partition_config.topic_belongs_to_partition(topic_name)) {
      return false;  // Skip - handled by another partition
    }

    std::string type_name = static_cast<std::string>(topic_entry["type"]);
    size_t queue_size = static_cast<int>(topic_entry["queue_size"]);
    if (!queue_size) {
      queue_size = 100;
    }

    // Get node for this topic (may create a new node if current is full)
    auto ros2_node = topic_node_manager.get_node_for_topic();

    LOG_INFO("[P%zu] Creating 2_to_1 bridge for topic '%s' with ROS 2 type '%s' on node '%s'",
      g_partition_config.partition_index, topic_name.c_str(), type_name.c_str(), ros2_node->get_name());

    try {
      ros1_bridge::BridgeHandles handles;
      if (topic_entry.hasMember("qos")) {
        LOG_INFO("Setting up QoS for '%s'", topic_name.c_str());
        auto qos_settings = qos_from_params(topic_entry["qos"]);
        handles.bridge2to1 = ros1_bridge::create_bridge_from_2_to_1(
          ros2_node, ros1_node,
          type_name, topic_name, queue_size, "", topic_name, queue_size);
        all_handles.push_back(handles);
      } else {
        handles.bridge2to1 = ros1_bridge::create_bridge_from_2_to_1(
          ros2_node, ros1_node,
          type_name, topic_name, queue_size, "", topic_name, queue_size);
        all_handles.push_back(handles);
      }
      return true;
    } catch (std::runtime_error & e) {
      LOG_ERROR("failed to create topics_2_to_1 bridge for topic '%s' with ROS 2 type '%s': %s",
        topic_name.c_str(), type_name.c_str(), e.what());
      return false;
    }
  };

  // ==================== OTHER TOPICS ====================
  // Now process all other topics (excluding /clock which is already done)
  LOG_INFO("========================================");
  LOG_INFO("Initializing remaining topics...");
  LOG_INFO("========================================");

  // Bidirectional topics (excluding /clock)
  if (has_topics) {
    for (size_t i = 0; i < static_cast<size_t>(topics.size()); ++i) {
      std::string topic_name = static_cast<std::string>(topics[i]["topic"]);
      if (topic_name == "/clock") {
        continue;  // Already processed
      }
      create_topic_bridge(topics[i]);
    }
  } else {
    LOG_WARN("The parameter '%s' either doesn't exist or isn't an array", topics_parameter_name);
  }

  // Topics 1 to 2
  XmlRpc::XmlRpcValue topics_1_to_2;
  if (
    ros1_node.getParam(topics_1_to_2_parameter_name, topics_1_to_2) &&
    topics_1_to_2.getType() == XmlRpc::XmlRpcValue::TypeArray)
  {
    for (size_t i = 0; i < static_cast<size_t>(topics_1_to_2.size()); ++i) {
      std::string topic_name = static_cast<std::string>(topics_1_to_2[i]["topic"]);

      // Check if this topic belongs to our partition
      if (!g_partition_config.topic_belongs_to_partition(topic_name)) {
        continue;  // Skip - handled by another partition
      }

      std::string type_name = static_cast<std::string>(topics_1_to_2[i]["type"]);
      size_t queue_size = static_cast<int>(topics_1_to_2[i]["queue_size"]);
      if (!queue_size) {
        queue_size = 100;
      }

      // Get node for this topic (may create a new node if current is full)
      auto ros2_node = topic_node_manager.get_node_for_topic();

      LOG_INFO("[P%zu] Creating 1_to_2 bridge for topic '%s' with ROS 2 type '%s' on node '%s'",
        g_partition_config.partition_index, topic_name.c_str(), type_name.c_str(), ros2_node->get_name());

      try {
        ros1_bridge::BridgeHandles handles;
        if (topics_1_to_2[i].hasMember("qos")) {
          LOG_INFO("Setting up QoS for '%s'", topic_name.c_str());
          auto qos_settings = qos_from_params(topics_1_to_2[i]["qos"]);
          handles.bridge1to2 = ros1_bridge::create_bridge_from_1_to_2(
            ros1_node, ros2_node,
            "", topic_name, queue_size, type_name, topic_name, qos_settings);
          all_handles.push_back(handles);
        } else {
          handles.bridge1to2 = ros1_bridge::create_bridge_from_1_to_2(
            ros1_node, ros2_node,
            "", topic_name, queue_size, type_name, topic_name, queue_size);
          all_handles.push_back(handles);
        }
      } catch (std::runtime_error & e) {
        LOG_ERROR("failed to create topics_1_to_2 bridge for topic '%s' with ROS 2 type '%s': %s",
          topic_name.c_str(), type_name.c_str(), e.what());
      }
    }
  } else {
    LOG_WARN("The parameter '%s' either doesn't exist or isn't an array", topics_1_to_2_parameter_name);
  }

  // Topics 2 to 1 (excluding /clock which is already processed)
  if (has_topics_2_to_1) {
    for (size_t i = 0; i < static_cast<size_t>(topics_2_to_1.size()); ++i) {
      std::string topic_name = static_cast<std::string>(topics_2_to_1[i]["topic"]);
      if (topic_name == "/clock") {
        continue;  // Already processed in priority section
      }
      create_2_to_1_bridge(topics_2_to_1[i]);
    }
  } else {
    LOG_WARN("The parameter '%s' either doesn't exist or isn't an array", topics_2_to_1_parameter_name);
  }

  // Flush all output before executor section
  fflush(stdout);
  fflush(stderr);

  LOG_INFO("========================================");
  if (g_partition_config.partition_count > 1) {
    LOG_INFO("[P%zu] All bridges initialized. Topics in this partition: %zu across %zu nodes",
      g_partition_config.partition_index,
      topic_node_manager.get_topic_count(), topic_node_manager.get_node_count());
  } else {
    LOG_INFO("All bridges initialized. Total topics: %zu across %zu nodes",
      topic_node_manager.get_topic_count(), topic_node_manager.get_node_count());
  }
  LOG_INFO("Starting executors...");
  LOG_INFO("========================================");

  // ROS 1 asynchronous spinner - use multiple threads for better throughput
  ros::AsyncSpinner async_spinner(4);
  async_spinner.start();

  LOG_INFO("ROS1 AsyncSpinner started with 4 threads");

  // ROS 2 spinning - use SEPARATE executors for TRUE waitset isolation
  // IMPORTANT: Each node needs its OWN executor in its OWN thread
  // Otherwise, adding multiple nodes to one executor merges their waitsets!
  //
  // Using SingleThreadedExecutor per node since:
  // 1. Node sharding already provides parallelism (multiple nodes = multiple threads)
  // 2. Each node handles ~1000 topics which is manageable for a single thread
  // 3. Avoids excessive thread creation from MultiThreadedExecutor

  // Keep executors alive for the duration of the program
  std::vector<std::shared_ptr<rclcpp::executors::SingleThreadedExecutor>> executors;
  std::vector<std::thread> executor_threads;

  // Dedicated executor for services (partition 0 only)
  std::shared_ptr<rclcpp::executors::SingleThreadedExecutor> service_executor;
  if (g_partition_config.partition_index == 0 && ros2_service_node) {
    service_executor = std::make_shared<rclcpp::executors::SingleThreadedExecutor>();
    service_executor->add_node(ros2_service_node);

    executor_threads.emplace_back([service_executor]() {
      LOG_INFO("ROS2 Service Executor started (isolated waitset, dedicated thread)");
      service_executor->spin();
    });
  }

  // ============================================================
  // Service Discovery Keepalive Thread (partition 0 only)
  // ============================================================
  // FastDDS Discovery Server can have stale discovery state at startup,
  // causing service calls to timeout even when the service is available.
  //
  // IMPORTANT: Recreating clients within the same process doesn't fix this
  // because the stale state is at the DDS participant level, not client level.
  //
  // The fix: Use subprocess to call `ros2 service call` which creates a
  // completely fresh DDS participant. This is exactly what works when you
  // manually run `ros2 service call /GetEntityState` from CLI.
  // ============================================================
  std::atomic<bool> keepalive_running{true};
  std::thread service_keepalive_thread;

  if (g_partition_config.partition_index == 0 && !service_bridges_1_to_2.empty()) {
    // Collect service info for keepalive warmup
    struct ServiceWarmupInfo {
      std::string service_name;
      std::string service_type;
    };
    std::vector<ServiceWarmupInfo> warmup_services;
    for (const auto& bridge : service_bridges_1_to_2) {
      warmup_services.push_back({bridge.service_name, bridge.service_type});
    }

    LOG_INFO("Starting service keepalive thread for %zu services (subprocess warmup)",
      warmup_services.size());

    service_keepalive_thread = std::thread([warmup_services, &keepalive_running]() {
      // Warmup interval - call subprocess periodically
      // Start with initial delay to let services initialize
      const int initial_delay_sec = 15;
      const int warmup_interval_sec = 60;  // Check every 60 seconds

      LOG_INFO("Service keepalive thread started (subprocess warmup every %ds, initial delay %ds)",
        warmup_interval_sec, initial_delay_sec);

      // Initial delay
      for (int i = 0; i < initial_delay_sec * 10 && keepalive_running.load(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }

      while (keepalive_running.load() && rclcpp::ok()) {
        LOG_INFO("[Keepalive] Running subprocess warmup for %zu services...", warmup_services.size());

        // Call each service via subprocess to force fresh DDS discovery
        // This mimics: ros2 service call /ServiceName service_type/srv/Type
        for (const auto& info : warmup_services) {
          if (!keepalive_running.load() || !rclcpp::ok()) {
            break;
          }

          // Build command - clear ROS env vars first to avoid mixing ROS1/ROS2 paths,
          // then source ROS2 setup and call service
          std::string cmd = "bash -c 'unset ROS_DISTRO AMENT_PREFIX_PATH CMAKE_PREFIX_PATH PYTHONPATH LD_LIBRARY_PATH && "
                           "source /home/ros2_ws/install/setup.bash && "
                           "timeout 5 ros2 service call " + info.service_name + " " +
                           info.service_type + "'";

          LOG_INFO("[Keepalive] Executing: %s", cmd.c_str());

          // Capture output by redirecting to a temp approach using popen
          std::string full_cmd = cmd + " 2>&1";
          FILE* pipe = popen(full_cmd.c_str(), "r");
          std::string output;
          if (pipe) {
            char buffer[256];
            while (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
              output += buffer;
            }
            int status = pclose(pipe);
            int exit_code = WEXITSTATUS(status);

            // Remove trailing newline for cleaner logging
            while (!output.empty() && (output.back() == '\n' || output.back() == '\r')) {
              output.pop_back();
            }

            if (exit_code == 0) {
              LOG_INFO("[Keepalive] Service '%s' warmup successful (exit=0)", info.service_name.c_str());
            } else {
              LOG_WARN("[Keepalive] Service '%s' warmup failed (exit=%d)", info.service_name.c_str(), exit_code);
              LOG_WARN("[Keepalive] Output: %s", output.empty() ? "(none)" : output.c_str());
            }
          } else {
            LOG_ERROR("[Keepalive] Failed to execute command for '%s'", info.service_name.c_str());
          }
        }

        LOG_INFO("[Keepalive] Subprocess warmup complete");

        // Sleep until next warmup interval
        for (int i = 0; i < warmup_interval_sec * 10 && keepalive_running.load(); ++i) {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
      }

      LOG_INFO("Service keepalive thread exiting");
    });
  }

  // Create a dedicated SingleThreadedExecutor + thread for each topic node
  // This ensures each node has its own isolated waitset (~1000 topics max)
  const auto& topic_nodes = topic_node_manager.get_all_nodes();
  LOG_INFO("[P%zu] Creating %zu topic node executors (SingleThreadedExecutor per node)...",
    g_partition_config.partition_index, topic_nodes.size());

  for (size_t i = 0; i < topic_nodes.size(); ++i) {
    auto executor = std::make_shared<rclcpp::executors::SingleThreadedExecutor>();
    executor->add_node(topic_nodes[i]);
    executors.push_back(executor);

    std::string node_name = topic_nodes[i]->get_name();

    if (i == 0) {
      // First node runs in main thread (keeps main thread busy)
      LOG_INFO("[P%zu] ROS2 Topic Executor for node '%s' will run in main thread",
        g_partition_config.partition_index, node_name.c_str());
    } else {
      // Additional nodes run in separate threads
      LOG_INFO("[P%zu] Creating background thread for node '%s'...",
        g_partition_config.partition_index, node_name.c_str());
      size_t partition_idx = g_partition_config.partition_index;
      executor_threads.emplace_back([executor, node_name, partition_idx]() {
        LOG_INFO("[P%zu] ROS2 Topic Executor for node '%s' started (isolated waitset)",
          partition_idx, node_name.c_str());
        executor->spin();
      });
    }
  }

  LOG_INFO("========================================");
  LOG_INFO("[P%zu] Started %zu background executor threads + main thread",
    g_partition_config.partition_index, executor_threads.size());
  LOG_INFO("========================================");
  fflush(stdout);
  fflush(stderr);

  // Main thread spins the first topic node executor
  if (!executors.empty()) {
    LOG_INFO("[P%zu] Main thread spinning executor for node '%s'...",
      g_partition_config.partition_index, topic_nodes[0]->get_name());
    fflush(stdout);

    // Spin until ROS1 or ROS2 shutdown is requested
    while (ros::ok() && rclcpp::ok()) {
      executors[0]->spin_some();
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    LOG_INFO("[P%zu] Main executor loop exited (ros::ok=%d, rclcpp::ok=%d)",
      g_partition_config.partition_index, ros::ok(), rclcpp::ok());
  } else {
    // No topics in this partition - just wait for shutdown
    LOG_INFO("[P%zu] No executors, waiting for shutdown...", g_partition_config.partition_index);
    while (ros::ok() && rclcpp::ok()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    LOG_INFO("[P%zu] Wait loop exited (ros::ok=%d, rclcpp::ok=%d)",
      g_partition_config.partition_index, ros::ok(), rclcpp::ok());
  }

  // Cleanup - cancel all executors and join threads
  // Stop keepalive thread first
  keepalive_running.store(false);
  if (service_keepalive_thread.joinable()) {
    LOG_INFO("Waiting for service keepalive thread to exit...");
    service_keepalive_thread.join();
    LOG_INFO("Service keepalive thread joined");
  }

  if (service_executor) {
    service_executor->cancel();
  }
  for (auto& exec : executors) {
    exec->cancel();
  }
  for (auto& thread : executor_threads) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  // If parent process, wait for child processes to terminate
  if (!g_partition_config.is_child_process && !g_partition_config.child_pids.empty()) {
    LOG_INFO("Parent process waiting for %zu child processes to terminate...",
      g_partition_config.child_pids.size());
    for (pid_t pid : g_partition_config.child_pids) {
      int status;
      waitpid(pid, &status, 0);
      LOG_INFO("Child process %d terminated with status %d", pid, status);
    }
  }

  LOG_INFO("[P%zu] Process exiting normally (PID: %d)",
    g_partition_config.partition_index, getpid());
  fflush(stdout);
  fflush(stderr);

  return 0;

  } catch (const std::exception& e) {
    LOG_ERROR("[P%zu] UNCAUGHT EXCEPTION: %s (PID: %d)",
      g_partition_config.partition_index, e.what(), getpid());
    fflush(stdout);
    fflush(stderr);
    return 1;
  } catch (...) {
    LOG_ERROR("[P%zu] UNCAUGHT UNKNOWN EXCEPTION (PID: %d)",
      g_partition_config.partition_index, getpid());
    fflush(stdout);
    fflush(stderr);
    return 1;
  }
}
