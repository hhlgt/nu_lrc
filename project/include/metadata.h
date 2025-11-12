#pragma once

#include "ec/utils.h"
#include "ec/erasure_code.h"
#include "ec/rs.h"
#include "ec/lrc.h"
#include "ec/pc.h"
#include "logger.h"

#define IF_SIMULATION true
#define IF_SIMULATE_CROSS_CLUSTER true
#define IF_TEST_TRHROUGHPUT false
#define IF_DEBUG true
#define IF_LOG_TO_FILE true
#define IF_DIRECT_FROM_NODE true    // proxy can directly access data from nodes in other clusters
#define SOCKET_PORT_OFFSET 500
#define STORAGE_SERVER_OFFSET 1000
// #define IN_MEMORY true
// #define MEMCACHED true
// #define REDIS true
#define COORDINATOR_PORT 12121
#define CLIENT_PORT 21212

namespace ECProject
{
  enum MultiStripePR
  {
    RAND,
    DISPERSED,
    AGGREGATED,
    HORIZONTAL,
    VERTICAL
  };

  struct ECSchema
  {
    bool partial_decoding;
    bool partial_scheme;
    bool repair_priority;
    bool repair_method;
    ECTYPE ec_type;
    ErasureCode *ec = nullptr;
    PlacementRule placement_rule;
    MultiStripePR multistripe_placement_rule;
    int x;
    size_t block_size;  // bytes
    float storage_overhead;

    ~ECSchema()
    {
      if (ec!= nullptr) {
        delete ec;
      }
    }

    void set_ec(ErasureCode *new_ec) {
      if (ec != nullptr) {
        delete ec;
        ec = nullptr;
      }
      ec = new_ec;
    }
  };

  struct ObjectInfo
  {
    size_t value_len;
    unsigned int access_rate;
    unsigned int access_cnt;
    unsigned int map2stripe;  // one object only in one stripe
  };
  
  struct Stripe
  {
    unsigned int stripe_id;
    ErasureCode *ec = nullptr;
    size_t block_size;
    // data blocks, local parity blocks, global parity blocks in order
    std::vector<unsigned int> blocks2nodes;
    std::vector<unsigned int> block_ids;
    std::vector<std::string> objects;   // in order with data blocks

    ~Stripe()
    {
      if (ec != nullptr) {
        delete ec;
        ec = nullptr;
      }
    }
  };
  
  struct Node
  {
    unsigned int node_id;
    std::string node_ip;
    int node_port;
    unsigned int map2cluster;
  };

  struct Cluster
  {
    unsigned int cluster_id;
    std::string proxy_ip;
    int proxy_port;
    std::vector<unsigned int> nodes;
    std::unordered_set<unsigned int> holding_stripe_ids;  
  };
  
  struct ParametersInfo
  {
    bool partial_decoding;
    bool partial_scheme;
    bool repair_priority;
    bool repair_method;
    ECTYPE ec_type;
    PlacementRule placement_rule;
    MultiStripePR multistripe_placement_rule;
    CodingParameters cp;
    size_t block_size;
    Logger::LogLevel loglevel;
  };

  struct ScaleParameters
  {
    float storage_overhead_upper_bound = 1.5;
    float gamma = 0.3;
    float change_rate = 0.1;
    int test_time = 1;
    bool optimized_recal = true;
  };
  
  struct PlacementInfo
  {
    bool isvertical = false;
    bool merged_flag = false;
    ECTYPE ec_type;
    size_t value_len;
    size_t block_size;
    std::string key;
    std::string client_ip;
    int client_port;
    int offset;   // get
    CodingParameters cp;
    std::vector<unsigned int> stripe_ids;
    std::vector<unsigned int> block_ids;
    std::vector<std::pair<unsigned int, std::pair<std::string, int>>> datanode_ip_port;
    
  };

  struct DeletePlan
  {
    std::vector<unsigned int> block_ids;
    std::vector<std::pair<std::string, int>> blocks_info;
  };
    
  struct MainRepairPlan
  {
    bool partial_decoding;
    bool partial_scheme;
    bool isvertical = false;
    ECTYPE ec_type;
    CodingParameters cp;
    unsigned int cluster_id;
    size_t block_size;
    std::vector<int> help_clusters_partial_less;
    std::vector<int> parity_blocks_index;
    std::vector<int> failed_blocks_index;
    std::vector<int> live_blocks_index;
    std::vector<std::vector<std::pair<int, std::pair<std::string, int>>>>
        help_clusters_blocks_info;
    std::vector<std::vector<unsigned int>> help_clusters_block_ids;
    std::vector<std::pair<int, std::pair<std::string, int>>>
        inner_cluster_help_blocks_info;
    std::vector<unsigned int> inner_cluster_help_block_ids;
    std::vector<std::pair<unsigned int, std::pair<std::string, int>>> new_locations;
    std::vector<unsigned int> failed_block_ids;
  };

  struct HelpRepairPlan
  {
    bool partial_decoding;
    bool partial_scheme;
    bool isvertical = false;
    bool partial_less = false;
    ECTYPE ec_type;
    CodingParameters cp;
    unsigned int cluster_id;
    size_t block_size;
    int main_proxy_port;
    std::string main_proxy_ip;
    std::vector<int> parity_blocks_index;
    std::vector<int> failed_blocks_index;
    std::vector<int> live_blocks_index;
    // the order of blocks index must be consistent with that in main_repair_plan
    std::vector<std::pair<int, std::pair<std::string, int>>> 
        inner_cluster_help_blocks_info;
    std::vector<unsigned int> inner_cluster_help_block_ids;
  };

  struct LocationInfo
  {
    unsigned int cluster_id;
    int proxy_port;
    std::string proxy_ip;
    std::vector<std::pair<int, std::pair<std::string, int>>> blocks_info;
    std::vector<unsigned int> block_ids;
  };
  
  struct MainRecalPlan
  {
    bool partial_decoding;
    bool isvertical = false;
    ECTYPE ec_type;
    CodingParameters cp;
    unsigned int cluster_id;
    size_t block_size;
    std::vector<unsigned int> new_parity_block_ids;
    std::vector<LocationInfo> help_clusters_info;
    std::vector<std::pair<int, std::pair<std::string, int>>> 
        inner_cluster_help_blocks_info;
    std::vector<unsigned int> inner_cluster_help_block_ids;
    std::vector<std::pair<int, std::pair<std::string, int>>> new_locations;
  };

  struct HelpRecalPlan
  {
    bool partial_decoding;
    bool isvertical = false;
    ECTYPE ec_type;
    CodingParameters cp;
    size_t block_size;
    int main_proxy_port;
    std::string main_proxy_ip;
    std::vector<int> new_parity_block_idxs;
    std::vector<std::pair<int, std::pair<std::string, int>>> 
        inner_cluster_help_blocks_info;
    std::vector<unsigned int> inner_cluster_help_block_ids;
  };

  struct RelocatePlan
  {
    size_t block_size;
    std::vector<unsigned int> blocks_to_move;
    std::vector<std::pair<unsigned int, std::pair<std::string, int>>> src_nodes;
    std::vector<std::pair<unsigned int, std::pair<std::string, int>>> des_nodes;
  };
  
  // response
  struct SetResp
  {
    std::string proxy_ip;
    int proxy_port;
  };

  struct StripesInfo
  {
    std::vector<unsigned int> stripe_ids;
    std::vector<int> stripe_wides;
  };

  struct RepairResp
  {
    double decoding_time{0};
    double cross_cluster_time{0};
    double repair_time{0};
    double meta_time{0};
    int cross_cluster_transfers{0};
    int io_cnt{0};
    bool success{false};
  };

  struct MergeResp
  {
    double computing_time{0};
    double cross_cluster_time{0};
    double merging_time{0};
    double meta_time{0};
    int cross_cluster_transfers{0};
    int io_cnt{0};
    bool ifmerged{false};
  };

  struct ScaleResp
  {
    double computing_time{0};
    double cross_cluster_time{0};
    double scaling_time{0};
    double meta_time{0};
    int cross_cluster_transfers{0};
    int io_cnt{0};
    float old_storage_overhead{0};
    float old_hotness{0};
    float new_storage_overhead{0};
    float new_hotness{0};
    bool ifscaled{false};
  };

  struct MainRepairResp
  {
    double decoding_time{0};
    double cross_cluster_time{0};
  };

  struct MainRecalResp
  {
    double computing_time{0};
    double cross_cluster_time{0};
  };

  struct RelocateResp
  {
    double cross_cluster_time{0};
  };
  
  ECFAMILY check_ec_family(ECTYPE ec_type);
  ErasureCode* ec_factory(ECTYPE ec_type, CodingParameters cp);
  RSCode* rs_factory(ECTYPE ec_type, CodingParameters cp);
  LocallyRepairableCode* lrc_factory(ECTYPE ec_type, CodingParameters cp);
  ProductCode* pc_factory(ECTYPE ec_type, CodingParameters cp);
  ErasureCode* clone_ec(ECTYPE ec_type, ErasureCode* ec);
  void parse_args(Logger* logger, ParametersInfo& paras, ScaleParameters& scale_paras, std::string config_file);
  int stripe_wide_after_merge(ParametersInfo paras, int step_size);
  std::string getStartTime();
  void deepcopy_codingparameters(const CodingParameters& src_cp, CodingParameters& des_cp);
}