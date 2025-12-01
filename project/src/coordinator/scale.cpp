#include "coordinator.h"

// if two vectors are equal
inline bool areEqual(const std::vector<int>& vec1, const std::vector<int>& vec2)
{
  if (vec1.size() != vec2.size()) {
    return false;
  }
  std::vector<int> sortedVec1 = vec1;
  std::vector<int> sortedVec2 = vec2;
  std::sort(sortedVec1.begin(), sortedVec1.end());
  std::sort(sortedVec2.begin(), sortedVec2.end());
  return sortedVec1 == sortedVec2;
}

// if set1 is a subset of set2
inline bool isSubset(const std::unordered_set<int>& set1,
    const std::unordered_set<int>& set2)
{
  for (int num : set1) {
    if (set2.find(num) == set2.end()) {
      return false;
    }
  }
  return true;
}

namespace ECProject {
  void Coordinator::compute_hotness(ScaleResp& resp)
  {
    float old_hotness = 0;
    float new_hotness = 0;
    float old_so = 0;
    int cnt = 0;
    for (auto& kv : stripe_table_) {
      Stripe& stripe = kv.second;
      auto lrc = dynamic_cast<LocallyRepairableCode*>(stripe.ec);
      float old_avg = 0;
      float new_avg = 0;
      for (auto key : stripe.objects)
      {
        ObjectInfo& obj = commited_object_table_[key];
        int old_access_rate = obj.access_rate;
        // update access rate
        obj.access_rate = obj.access_cnt;
        obj.access_cnt = 0;
        // update hotness
        old_avg += old_access_rate * (obj.value_len / ec_schema_.block_size);
        new_avg += obj.access_rate * (obj.value_len / ec_schema_.block_size);
      }
      old_avg /= lrc->k;
      new_avg /= lrc->k;
      old_hotness += old_avg;
      new_hotness += new_avg;
      old_so += stripe.ec->storage_overhead;
      ++cnt;
    }
    old_hotness /= cnt;
    new_hotness /= cnt;
    old_so /= cnt;
    resp.old_hotness = old_hotness;
    resp.new_hotness = new_hotness;
    resp.old_storage_overhead = old_so;
    resp.new_storage_overhead = old_so;
  }

  std::vector<unsigned int> Coordinator::stripes_for_scaling(
          float storage_overhead_upper, float gamma, ScaleResp& resp)
  {
    std::vector<unsigned int> stripe_ids;
    int cnt = 0;
    float old_hotness = 0;
    float new_hotness = 0;
    float old_so = 0;
    float new_so = 0;
    for (auto& kv : stripe_table_) {
      Stripe& stripe = kv.second;
      auto lrc = dynamic_cast<LocallyRepairableCode*>(stripe.ec);
      bool flag = false;
      float old_avg = 0;
      float new_avg = 0;
      for (auto key : stripe.objects)
      {
        ObjectInfo& obj = commited_object_table_[key];
        int old_access_rate = obj.access_rate;
        // update access rate
        obj.access_rate = obj.access_cnt;
        obj.access_cnt = 0;
        if ((std::fabs((float)obj.access_rate / (float)obj.access_cnt) - 1.0) > gamma) {
          flag = true;
        }
        old_avg += old_access_rate * (obj.value_len / ec_schema_.block_size);
        new_avg += obj.access_rate * (obj.value_len / ec_schema_.block_size);
      }
      old_avg /= lrc->k;
      new_avg /= lrc->k;
      old_hotness += old_avg;
      new_hotness += new_avg;
      old_so += stripe.ec->storage_overhead;
      if (flag) {
        int new_l = int(round((new_avg * (float)lrc->l) / old_avg));
        float new_storage_overhead = 
            std::min((float)(lrc->k + lrc->g + new_l) / (float)lrc->k, storage_overhead_upper);
        stripe.ec->storage_overhead = new_storage_overhead;
        stripe_ids.push_back(stripe.stripe_id);
      }
      new_so += stripe.ec->storage_overhead;
      ++cnt;
    }
    old_hotness /= cnt;
    new_hotness /= cnt;
    old_so /= cnt;
    new_so /= cnt;
    resp.old_hotness = old_hotness;
    resp.new_hotness = new_hotness;
    resp.old_storage_overhead = old_so;
    resp.new_storage_overhead = new_so;
    return stripe_ids;
  }
  void Coordinator::do_scaling(const std::vector<unsigned int>& stripe_ids, ScaleResp& resp, bool optimized_recal)
  {
    struct timeval start_time, end_time;
    struct timeval m_start_time, m_end_time;
    double scaling_time = 0;
    double computing_time = 0;
    double cross_cluster_time = 0;
    double meta_time = 0;
    int cross_cluster_transfers = 0;
    int io_cnt = 0;
    int stripe_num = (int)stripe_ids.size();
    int stripe_cnt = 0;
    size_t block_size = ec_schema_.block_size;

    for (auto& t_stripe_id : stripe_ids) {
      gettimeofday(&start_time, NULL);
      gettimeofday(&m_start_time, NULL);
      Stripe& stripe = stripe_table_[t_stripe_id];
      std::vector<size_t> file_sizes;
      std::vector<unsigned int> file_access_rates;
      for (auto key : stripe.objects)
      {
        ObjectInfo& obj = commited_object_table_[key];
        file_sizes.push_back(obj.value_len);
        file_access_rates.push_back(obj.access_rate);
      }

      std::vector<unsigned int> old_placement_info;
      old_placement_info.insert(old_placement_info.end(),
          stripe.blocks2nodes.begin(), stripe.blocks2nodes.end());

      find_out_stripe_partitions(stripe.stripe_id);
      if (IF_DEBUG) {
        auto str = stripe.ec->print_info(stripe.ec->partition_plan, "partition");
        write_logs(Logger::LogLevel::DEBUG, str);
      }

      auto old_partition_plan = stripe.ec->partition_plan;

      CodingParameters cp;
      stripe.ec->get_coding_parameters(cp);
      auto old_ec = stripe.ec;
      auto new_ec = clone_ec(ec_schema_.ec_type, stripe.ec);
      new_ec->init_coding_parameters(cp);
      auto lrc = dynamic_cast<Non_Uni_LRC*>(new_ec);
      lrc->generate_coding_parameters_for_a_stripe(file_sizes,
          file_access_rates, stripe.block_size);
      auto old_lrc = dynamic_cast<Non_Uni_LRC*>(old_ec);
      bool needed = false;
      int len = lrc->krs.size();
      for (int ii = 0; ii < len; ++ii) {
        if (lrc->krs[ii].first != old_lrc->krs[ii].first ||
          lrc->krs[ii].second != old_lrc->krs[ii].second) {
            needed = true;
            break;
          }
      }
      if (!needed) {
        continue;
      }
      lrc->generate_partition();
      if (IF_DEBUG) {
        auto info = "Before scaling: " + old_ec->self_information()
                    +  "\nAfter scaling: " + lrc->self_information() + "\n";
        write_logs(Logger::LogLevel::INFO, info);
        auto str = lrc->print_info(lrc->partition_plan, "partition");
        write_logs(Logger::LogLevel::DEBUG, str);
      }

      // generate recalculation plans
      std::vector<std::vector<int>> recalculation_plans;
      if (optimized_recal) {
        generate_recalculation_plans(recalculation_plans, old_ec, new_ec);
      } else {
        lrc->grouping_information(recalculation_plans);
        for (auto& vec : recalculation_plans) {
          int threshold = lrc->k + lrc->g;
          vec.erase(std::remove_if(vec.begin(), vec.end(), [threshold](int val) {
              return val >= threshold;}), vec.end());
        }
      }
      if (IF_DEBUG) {
        std::string msg = "Recalculation Plans:\n";
        int idx = 0;
        for (auto& plan : recalculation_plans) {
          msg += std::to_string(idx) + ": ";
          for (auto num : plan) {
            msg += std::to_string(num) + " ";
          }
          msg += "\n";
          ++idx;
        }
        write_logs(Logger::LogLevel::INFO, msg);
      }

      // generate new placement info
      int new_stripe_wide = new_ec->k + new_ec->m;
      std::vector<unsigned int> new_block_ids(new_stripe_wide, UINT_MAX);
      for (int i = 0; i < lrc->k + lrc->g; i++) {
        new_block_ids[i] = stripe.block_ids[i];
      }
      std::unordered_map<int, int> new2old;
      int gid = 0;
      for (auto& vec : recalculation_plans) {
        if (vec.size() == 1 && vec[0] >= lrc->k + lrc->g) {
          new2old[lrc->k + lrc->g + gid] = vec[0];
          new_block_ids[lrc->k + lrc->g + gid] = stripe.block_ids[vec[0]];
        } 
        ++gid;
      }
      for (int i = lrc->k + lrc->g; i < new_stripe_wide; i++) {
        if (new_block_ids[i] == UINT_MAX) {
          new_block_ids[i] = cur_block_id_++;
        }
      }
      std::vector<unsigned int> new_placement_info;
      if (stripe.ec->placement_rule == PlacementRule::FLAT) {
        new_placement_info = new_placement_for_partitions_flat(
            new_ec, old_placement_info, new2old);
      } else {
        auto new_partition_plan = new_ec->partition_plan;
        new_placement_info = new_placement_for_partitions(
            false, new_ec, old_partition_plan,
            new_partition_plan, old_placement_info, new2old);
      }
      
      // local parity blocks for delete
      DeletePlan old_parities;
      std::unordered_set<unsigned int> old_parities_cluster_set;
      int old_stripe_wide = (int)old_placement_info.size();
      std::unordered_set<int> old_locals;
      for (int i = lrc->k + lrc->g; i < old_stripe_wide; i++) {
        old_locals.insert(i);
      }
      for (auto& kv : new2old) {
        old_locals.erase(kv.second);
      }
      for (auto i : old_locals) {
        Node& t_node = node_table_[old_placement_info[i]];
        old_parities.blocks_info.push_back(
            std::make_pair(t_node.node_ip, t_node.node_port));
        old_parities.block_ids.push_back(stripe.block_ids[i]);
        old_parities_cluster_set.insert(t_node.map2cluster);
      }

      if (IF_DEBUG) {
        std::string msg = "Delete old local parirties: ";
        for (auto i : old_locals) {
          msg += std::to_string(i) + "[" + std::to_string(stripe.block_ids[i]) + "] ";
        }
        msg += "\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }

      // info for recalculation local parity blocks parallelly
      MainRecalPlan main_plan;
      int group_num = (int)recalculation_plans.size();
      std::vector<MainRecalPlan> main_plans(group_num);
      std::vector<unsigned int> main_cluster_ids(group_num);
      for (int i = 0; i < group_num; i++) {
        if (recalculation_plans[i].size() == 1 &&
            recalculation_plans[i][0] >= lrc->k + lrc->g) { // only metadata update needed
          continue;
        }
        for (auto bid : recalculation_plans[i]) {
          unsigned int t_node_id = old_placement_info[bid];
          unsigned int t_cluster_id = node_table_[t_node_id].map2cluster;
          bool is_in = false;
          for (auto& t_location : main_plans[i].help_clusters_info) {
            if (t_location.cluster_id == t_cluster_id) {
              Node& t_node = node_table_[t_node_id];
              t_location.blocks_info.push_back(
                  std::make_pair(bid,
                  std::make_pair(t_node.node_ip, t_node.node_port)));
              t_location.block_ids.push_back(stripe.block_ids[bid]);
              is_in = true;
              break;
            }
          }
          if (!is_in) {
            LocationInfo new_location;
            new_location.cluster_id = t_cluster_id;
            new_location.proxy_port = cluster_table_[t_cluster_id].proxy_port;
            new_location.proxy_ip = cluster_table_[t_cluster_id].proxy_ip;
            Node& t_node = node_table_[t_node_id];
            new_location.blocks_info.push_back(
                std::make_pair(bid,
                std::make_pair(t_node.node_ip, t_node.node_port)));
            new_location.block_ids.push_back(stripe.block_ids[bid]);
            main_plans[i].help_clusters_info.push_back(new_location);
          }
        }
        int lid = lrc->k + lrc->g + i;
        unsigned int t_node_id = new_placement_info[lid];
        Node& t_node = node_table_[t_node_id];
        unsigned int t_cluster_id = t_node.map2cluster;
        main_cluster_ids[i] = t_cluster_id;
        main_plans[i].new_locations.push_back(
            std::make_pair(lid, std::make_pair(t_node.node_ip, t_node.node_port)));
        main_plans[i].new_parity_block_ids.push_back(new_block_ids[lid]);
      }
      if (IF_DEBUG) {
        std::string msg = "Calculate new local parirties: ";
        for (int i = 0; i < group_num; i++) {
          int lid = lrc->k + lrc->g + i;
          msg += std::to_string(lid) + "[" + std::to_string(new_block_ids[lid]) + "] ";
        }
        msg += "\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }
      if (IF_DEBUG) {
        std::string msg = "\nOld placement: ";
        for (auto i = 0; i < old_stripe_wide; ++i) {
          msg += std::to_string(stripe.block_ids[i]) + "[" + std::to_string(stripe.blocks2nodes[i]) + "] ";
        }
        msg += "\nNew placement: ";
        for (auto i = 0; i < new_stripe_wide; ++i) {
          msg += std::to_string(new_block_ids[i]) + "[" + std::to_string(new_placement_info[i]) + "] ";
        }
        msg += "\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }

      // info for block relocation
      std::vector<unsigned int> blocks_to_move;
      std::vector<unsigned int> src_nodes;
      std::vector<unsigned int> des_nodes;
      for (int i = 0; i < lrc->k + lrc->g; i++) {
        int src_node_id = old_placement_info[i];
        int des_node_id = new_placement_info[i];
        if (src_node_id != des_node_id) {
          blocks_to_move.push_back(stripe.block_ids[i]);
          src_nodes.push_back(src_node_id);
          des_nodes.push_back(des_node_id);
        }
      }
      for (auto& kv : new2old) {  // reusable local parities
        int src_node_id = old_placement_info[kv.second];
        int des_node_id = new_placement_info[kv.first];
        if (src_node_id != des_node_id) {
          blocks_to_move.push_back(stripe.block_ids[kv.second]);
          src_nodes.push_back(src_node_id);
          des_nodes.push_back(des_node_id);
        }
      }
      RelocatePlan reloc_plan;
      reloc_plan.block_size = block_size;
      int move_num = (int)blocks_to_move.size();
      if (move_num > 0) {
        for (int i = 0; i < move_num; i++) {
          reloc_plan.blocks_to_move.push_back(blocks_to_move[i]);
          auto& src_node = node_table_[src_nodes[i]];
          reloc_plan.src_nodes.push_back(std::make_pair(src_node.map2cluster,
              std::make_pair(src_node.node_ip, src_node.node_port)));
          auto& des_node = node_table_[des_nodes[i]];
          reloc_plan.des_nodes.push_back(std::make_pair(des_node.map2cluster,
              std::make_pair(des_node.node_ip, des_node.node_port)));
          if (src_node.map2cluster != des_node.map2cluster) {
            cross_cluster_time++;
          }
        }
        if (IF_DEBUG) {
          std::string msg = "[Scaling] " + std::to_string(move_num) + " blocks to relocate:\n";
          for (int ii = 0; ii < move_num; ii++) {
            msg += std::to_string(blocks_to_move[ii]) + "[" +
                std::to_string(src_nodes[ii]) + "->" +
                std::to_string(des_nodes[ii]) + "] ";
          }
          msg += "\n";
          write_logs(Logger::LogLevel::INFO, msg);
        }
      }
      gettimeofday(&m_end_time, NULL);
      meta_time += m_end_time.tv_sec - m_start_time.tv_sec +
          (m_end_time.tv_usec - m_start_time.tv_usec) / 1000000;

      for (int i = 0; i < group_num; i++) {
        if (recalculation_plans[i].size() == 1 &&
            recalculation_plans[i][0] >= lrc->k + lrc->g) { // only metadata update needed
          continue;
        }
        auto& main_plan = main_plans[i];
        unsigned int parity_cluster_id = main_cluster_ids[i];
        main_plan.ec_type = ec_schema_.ec_type;
        new_ec->get_coding_parameters(main_plan.cp);
        main_plan.cp.local_or_column = true;
        main_plan.cluster_id = parity_cluster_id;
        main_plan.block_size = ec_schema_.block_size;
        main_plan.partial_decoding = ec_schema_.partial_decoding;
        int ci = 0;
        for (auto& location : main_plan.help_clusters_info) {
          if (location.cluster_id == parity_cluster_id) {
            main_plan.inner_cluster_help_blocks_info.insert(
                main_plan.inner_cluster_help_blocks_info.end(),
                location.blocks_info.begin(),  location.blocks_info.end());
            main_plan.inner_cluster_help_block_ids.insert(
                main_plan.inner_cluster_help_block_ids.end(),
                location.block_ids.begin(),  location.block_ids.end());
            main_plan.help_clusters_info.erase(main_plan.help_clusters_info.begin() + ci);
            break;
          }
          ci++;
        }
          
        // simulate recalculation
        simulation_recalculation(main_plan, cross_cluster_transfers, io_cnt);

        auto lock_ptr = std::make_shared<std::mutex>();
        auto send_main_plan = [this, main_plan, parity_cluster_id,
                                lock_ptr, &computing_time,
                                &cross_cluster_time]() mutable
        {
          std::string chosen_proxy = cluster_table_[parity_cluster_id].proxy_ip +
              std::to_string(cluster_table_[parity_cluster_id].proxy_port);
          auto resp = async_simple::coro::syncAwait(proxies_[chosen_proxy]
                          ->call<&Proxy::main_recal>(main_plan)).value();
          lock_ptr->lock();
          computing_time += resp.computing_time;
          cross_cluster_time += resp.cross_cluster_time;
          lock_ptr->unlock();
          if (IF_DEBUG) {
            std::string msg = "Send main plan to proxy " + chosen_proxy + "\n";
            write_logs(Logger::LogLevel::INFO, msg);
          }
        };

        auto logger_lock_ptr = std::make_shared<std::mutex>();
        auto send_help_plan = [this, block_size, main_plan,
                              parity_cluster_id, logger_lock_ptr](int help_idx)
        {
          HelpRecalPlan help_plan;
          help_plan.ec_type = ec_schema_.ec_type;
          deepcopy_codingparameters(main_plan.cp, help_plan.cp);
          help_plan.block_size = ec_schema_.block_size;
          help_plan.partial_decoding = ec_schema_.partial_decoding;
          help_plan.cp.local_or_column = main_plan.cp.local_or_column;
          help_plan.main_proxy_port = cluster_table_[parity_cluster_id].proxy_port + SOCKET_PORT_OFFSET;
          help_plan.main_proxy_ip = cluster_table_[parity_cluster_id].proxy_ip;
          auto& location = main_plan.help_clusters_info[help_idx];
          unsigned int cluster_id = location.cluster_id;
          help_plan.inner_cluster_help_blocks_info = location.blocks_info;
          help_plan.inner_cluster_help_block_ids = location.block_ids;
          for (int i = 0; i < int(main_plan.new_locations.size()); i++) {
            help_plan.new_parity_block_idxs.push_back(main_plan.new_locations[i].first);
          }
          std::string chosen_proxy = cluster_table_[cluster_id].proxy_ip +
              std::to_string(cluster_table_[cluster_id].proxy_port);
          async_simple::coro::syncAwait(proxies_[chosen_proxy]
              ->call<&Proxy::help_recal>(help_plan));
          if (IF_DEBUG) {
            logger_lock_ptr->lock();
            std::string msg = "Send help plan to proxy " + chosen_proxy + "\n";
            write_logs(Logger::LogLevel::INFO, msg);
            logger_lock_ptr->unlock();
          }
        };

        if (!IF_SIMULATION) {
          // recalculation
          try {
            if (IF_DEBUG) {
              std::string msg = "[Parities Recalculation] Send main and help proxy plans!\n";
              write_logs(Logger::LogLevel::INFO, msg);
            }  
            std::thread my_main_thread(send_main_plan);
            std::vector<std::thread> senders;
            int i = 0;
            int new_parity_num = (int)main_plan.new_parity_block_ids.size();
            for (auto& location : main_plan.help_clusters_info) {
              if ((IF_DIRECT_FROM_NODE
                  && new_parity_num < location.block_ids.size()
                  && ec_schema_.partial_decoding) || !IF_DIRECT_FROM_NODE) {
                senders.push_back(std::thread(send_help_plan, i));
              }
              i++;
            }
            for (int j = 0; j < int(senders.size()); j++) {
              senders[j].join();
            }
            my_main_thread.join();
          }
          catch (const std::exception &e)
          {
            std::cerr << e.what() << '\n';
          }
        }
      }

      if (!IF_SIMULATION) {
        if (IF_DEBUG) {
          std::string msg = "Finish recalculation and begin to relocate blocks!\n";
          write_logs(Logger::LogLevel::INFO, msg);
        }
        // block relocation
        if (move_num > 0) {
          unsigned int ran_cluster_id = (unsigned int)random_index(num_of_clusters_);
          std::string chosen_proxy = cluster_table_[ran_cluster_id].proxy_ip +
            std::to_string(cluster_table_[ran_cluster_id].proxy_port);
          auto resp = async_simple::coro::syncAwait(proxies_[chosen_proxy]
                        ->call<&Proxy::block_relocation>(reloc_plan)).value();
          cross_cluster_time += resp.cross_cluster_time;
          io_cnt += 2 * (int)reloc_plan.blocks_to_move.size();
        }

        if (IF_DEBUG) {
          std::string msg = "Finish relocation and delete old local parity blocks!\n";
          write_logs(Logger::LogLevel::INFO, msg);
        }
        // delete old parities
        int idx = random_index(old_parities_cluster_set.size());
        int del_cluster_id = *(std::next(old_parities_cluster_set.begin(), idx));
        std::string chosen_proxy = cluster_table_[del_cluster_id].proxy_ip +
            std::to_string(cluster_table_[del_cluster_id].proxy_port);
        async_simple::coro::syncAwait(proxies_[chosen_proxy]
            ->call<&Proxy::delete_blocks>(old_parities));
      }

      if (IF_DEBUG) {
        std::string msg = "Update metadata!\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }
      // update stripe info
      gettimeofday(&m_start_time, NULL);
      stripe.blocks2nodes.resize(new_stripe_wide);
      stripe.block_ids.resize(new_stripe_wide);
      for (auto i = 0; i < new_stripe_wide; i++) {
        stripe.blocks2nodes[i] = new_placement_info[i];
        stripe.block_ids[i] = new_block_ids[i];
      }
      stripe.ec = clone_ec(ec_schema_.ec_type, new_ec);
      gettimeofday(&m_end_time, NULL);
      meta_time += m_end_time.tv_sec - m_start_time.tv_sec +
          (m_end_time.tv_usec - m_start_time.tv_usec) / 1000000;
      gettimeofday(&end_time, NULL);
      double temp_time = end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) * 1.0 / 1000000;
      scaling_time += temp_time;
      std::string msg = "[Scaling] Stripe " + std::to_string(stripe.stripe_id) 
                        + " total_time:" + std::to_string(scaling_time) + " latest:"
                        + std::to_string(temp_time) + "\n";
      write_logs(Logger::LogLevel::INFO, msg);
      stripe_cnt++;
    }
    resp.computing_time = computing_time;
    resp.cross_cluster_time = cross_cluster_time;
    resp.meta_time = meta_time;
    resp.scaling_time = scaling_time;
    resp.cross_cluster_transfers = cross_cluster_transfers;
    resp.io_cnt = io_cnt;
    resp.ifscaled = true;
    if (IF_DEBUG) {
      for (auto& kv : stripe_table_) {
        std::string msg = "Stripe " + std::to_string(kv.first) + " : ";
        msg += kv.second.ec->self_information() + "\n";
        for (auto& key : kv.second.objects) {
          ObjectInfo& obj = commited_object_table_[key];
          msg += key + "[" + std::to_string(obj.value_len / ec_schema_.block_size) + "," + std::to_string(obj.access_rate) + "] ";
        }
        msg += "\n";
        write_logs(Logger::LogLevel::INFO, msg);
      }
    }
  }

  void Coordinator::generate_recalculation_plans(
          std::vector<std::vector<int>>& plans,
          ErasureCode* old_ec, ErasureCode* new_ec)
  {
    auto old_lrc = dynamic_cast<Non_Uni_LRC*>(old_ec);
    auto new_lrc = dynamic_cast<Non_Uni_LRC*>(new_ec);
    int k = new_lrc->k;
    int g = new_lrc->g;

    std::vector<std::vector<int>> old_groups;
    std::vector<std::vector<int>> new_groups;
    old_lrc->grouping_information(old_groups);
    new_lrc->grouping_information(new_groups);

    std::unordered_map<int, std::vector<int>> old_file2groups;
    std::unordered_map<int, std::vector<int>> new_file2groups;
    int gid = 0;
    for (int i = 0; i < (int)old_lrc->krs.size(); ++i) {
      int ki = old_lrc->krs[i].first;
      int ri = old_lrc->krs[i].second;
      if (ri != 0) {
        for (int j = 0; j < ki / ri; ++j) {
          old_file2groups[i].emplace_back(gid++);
        }
      }
    }
    gid = 0;
    for (int i = 0; i < (int)new_lrc->krs.size(); ++i) {
      int ki = new_lrc->krs[i].first;
      int ri = new_lrc->krs[i].second;
      if (ri != 0) {
        for (int j = 0; j < ki / ri; ++j) {
          new_file2groups[i].emplace_back(gid++);
        }
      }
    }
    gid = 0;
    int tplus1 = new_lrc->krs.size();
    for (int i = 0; i < tplus1; ++i) {
      int new_ki = new_lrc->krs[i].first;
      int new_ri = new_lrc->krs[i].second;
      int old_ri = old_lrc->krs[i].second;
      if (new_ri == 0) {
        continue;
      }
      if (new_ri >= old_ri) {
        int gni = new_ki / new_ri;
        if (i == tplus1 - 1 && new_ki % new_ri != 0) {
          ++gni;
        }
        for (int j = 0; j < gni; ++j) {
          plans.emplace_back(std::vector<int>());
          auto new_group = std::unordered_set<int>(new_groups[gid].begin(),
              new_groups[gid].end());
          new_group.erase(k + g + gid);
          for (auto& o_gid : old_file2groups[i]) {
            auto old_group = std::unordered_set<int>(old_groups[o_gid].begin(),
                old_groups[o_gid].end());
            old_group.erase(k + g + o_gid);
            if (isSubset(old_group, new_group)) {
              plans[gid].emplace_back(k + g + o_gid);
              for (auto num : old_group) {
                new_group.erase(num);
              }
            }
          }
          for (auto num : new_group) {
            plans[gid].emplace_back(num);
          }
          gid++;
        }
      } else {
        int gni = new_ki / new_ri;
        if (i == tplus1 - 1 && new_ki % new_ri != 0) {
          ++gni;
        }
        for (int j = 0; j < gni; ++j) {
          if (new_ri > old_ri / 2) {
            plans.emplace_back(std::vector<int>());
            auto new_group = std::unordered_set<int>(new_groups[gid].begin(),
              new_groups[gid].end());
            new_group.erase(k + g + gid);
            bool flag = false;
            for (auto& o_gid : old_file2groups[i]) {
              auto old_group = std::unordered_set<int>(old_groups[o_gid].begin(),
                  old_groups[o_gid].end());
              old_group.erase(k + g + o_gid);
              if (isSubset(new_group, old_group)) {
                plans[gid].emplace_back(k + g + o_gid);
                for (const auto& num : old_group) {
                  if (new_group.find(num) == new_group.end()) {
                    plans[gid].emplace_back(num);
                  }
                }
                flag = true;
                break;
              }
            }
            if (!flag) {
              for (auto num : new_groups[gid]) {
                if (num != k + g + gid) {
                  plans[gid].emplace_back(num);
                }
              }
            }
            ++gid;
          } else {
            plans.emplace_back(std::vector<int>());
            for (auto num : new_groups[gid]) {
              if (num != k + g + gid) {
                plans[gid].emplace_back(num);
              }
            }
            ++gid;
          }
        }
      }
    }
  }

  std::vector<int> Coordinator::find_most_common_partitions(
        const std::vector<std::vector<int>>& par1,
        const std::vector<std::vector<int>>& par2)
  {
    std::vector<std::unordered_set<int>> setList;
    for (const auto& row : par2) {
      setList.emplace_back(row.begin(), row.end());
    }
    
    std::vector<int> resultIndices;
    std::unordered_set<int> usedIndices;
    
    for (const auto& row1 : par1) {
      int maxIntersectionSize = 0;
      int bestIndex = -1;
      std::unordered_set<int> currentSet(row1.begin(), row1.end());
        
      for (size_t i = 0; i < setList.size(); ++i) {
        int intersectionSize = std::count_if(currentSet.begin(), currentSet.end(),
            [&](int num) {
                return setList[i].find(num) != setList[i].end();
            });
        if (intersectionSize > maxIntersectionSize &&
            usedIndices.find(i) == usedIndices.end()) {
          maxIntersectionSize = intersectionSize;
          bestIndex = static_cast<int>(i);
        }
      }

      if (bestIndex != -1) {
        resultIndices.push_back(bestIndex);
        usedIndices.insert(bestIndex);
      } else {
        resultIndices.push_back(-1);
      }
    }
    return resultIndices;
  }

  std::vector<int> Coordinator::find_out_same_partitions(
            const std::vector<std::vector<int>>& par1,
            const std::vector<std::vector<int>>& par2)
  {
    std::vector<int> resultIndices;
    for (const auto& row1 : par1) {
      int matchIndex = -1;
      for (size_t i = 0; i < par2.size(); ++i) {
        if (areEqual(row1, par2[i])) {
          matchIndex = static_cast<int>(i);
          break;
        }
      }
      resultIndices.push_back(matchIndex);
    }
    return resultIndices;
  }

  std::vector<unsigned int> Coordinator::new_placement_for_partitions_flat(
            ErasureCode* new_ec,
            const std::vector<unsigned int>& old_placement_info,
            const std::unordered_map<int, int>& new2old)
  {
    int stripe_wide = new_ec->k + new_ec->m;
    unsigned int unreached_node_id = num_of_clusters_ * num_of_nodes_per_cluster_ + 1;
    std::vector<unsigned int> new_placement_info(stripe_wide, unreached_node_id); 
    auto lrc = dynamic_cast<LocallyRepairableCode*>(new_ec);

    std::vector<unsigned int> free_clusters;
    for (unsigned int i = 0; i < num_of_clusters_; i++) {
      free_clusters.push_back(i);
    }

    for (int i = 0; i < lrc->k + lrc->g; i++) {
      auto nid = old_placement_info[i];
      new_placement_info[i] = nid;
      auto cid = node_table_[nid].map2cluster;
      auto it = std::find(free_clusters.begin(), free_clusters.end(), cid);
      if (it!= free_clusters.end()) {
        free_clusters.erase(it); 
      }
    }

    for (int i = lrc->k + lrc->g; i < stripe_wide; i++) {
      if (new2old.find(i) != new2old.end()) {
        int old_idx = new2old.at(i);
        auto nid = old_placement_info[old_idx];
        new_placement_info[i] = nid;
        auto cid = node_table_[nid].map2cluster;
        auto it = std::find(free_clusters.begin(), free_clusters.end(), cid);
        if (it!= free_clusters.end()) {
          free_clusters.erase(it);
        }
      } 
    }
    for (int i = lrc->k + lrc->g; i < stripe_wide; i++) {
      if (new_placement_info[i] == unreached_node_id) {
        int idx = random_index(free_clusters.size());
        unsigned int cid = free_clusters[idx]; 
        free_clusters.erase(free_clusters.begin() + idx);
        unsigned int nid = cid * num_of_nodes_per_cluster_ + random_index(num_of_nodes_per_cluster_);
        new_placement_info[i] = nid;
      } 
    }

    for (auto nid : new_placement_info) {
      my_assert(nid != unreached_node_id);
    }

    return new_placement_info;
  }

  std::vector<unsigned int> Coordinator::new_placement_for_partitions(
            bool if_common,
            ErasureCode* new_ec,
            const std::vector<std::vector<int>>& old_pars,
            const std::vector<std::vector<int>>& new_pars,
            const std::vector<unsigned int>& old_placement_info,
            const std::unordered_map<int, int>& new2old)
  {
    int stripe_wide = new_ec->k + new_ec->m;
    unsigned int unreached_node_id = num_of_clusters_ * num_of_nodes_per_cluster_ + 1;
    std::vector<unsigned int> new_placement_info(stripe_wide, unreached_node_id);
    auto lrc = dynamic_cast<LocallyRepairableCode*>(new_ec);

    std::vector<unsigned int> old_par2cid;
    for (const auto& par : old_pars) {
      int bid = par[0];
      unsigned int cid = node_table_[old_placement_info[bid]].map2cluster;
      old_par2cid.push_back(cid);
    }

    std::vector<std::vector<int>> new_pars_copy;
    for (auto& par : new_pars) {
      std::vector<int> vec;
      for (auto& bid : par) {
        if (bid >= lrc->k + lrc->g && new2old.find(bid) != new2old.end()) {
          vec.emplace_back(new2old.at(bid));
        } else {
          vec.emplace_back(bid);
        }
      }
      new_pars_copy.emplace_back(vec);
    }

    if (if_common) {
      std::vector<int> common_pars = find_most_common_partitions(new_pars_copy, old_pars);

      if (IF_DEBUG) {
        std::string msg = "common_pars: ";
        for (auto par_id : common_pars) {
          msg += std::to_string(par_id) + " ";
        }
        msg += "\n";
        write_logs(Logger::LogLevel::DEBUG, msg);
      }

      std::vector<unsigned int> free_clusters;
      for (unsigned int i = 0; i < num_of_clusters_; i++) {
        free_clusters.push_back(i);
      }
      for (int i = 0; i < (int)new_pars.size(); i++) {
        int par_id = common_pars[i];
        if (par_id != -1) {
          int cid = old_par2cid[par_id];
          Cluster& cluster = cluster_table_[cid];
          std::vector<unsigned int> free_nodes;
          for (int j = 0; j < num_of_nodes_per_cluster_; j++) {
            free_nodes.push_back(cluster.nodes[j]);
          }
          std::vector<int> uncommon_bids;
          for (int bid : new_pars[i]) {
            int old_bid = bid;
            if (bid >= lrc->k + lrc->g && new2old.find(bid)!= new2old.end()) {
              old_bid = new2old.at(bid);
            }
            auto it = std::find(old_pars[par_id].begin(), old_pars[par_id].end(), old_bid);
            if (it == old_pars[par_id].end()) {
              uncommon_bids.push_back(bid);
            } else {
              new_placement_info[bid] = old_placement_info[bid];
              auto iter = std::find(free_nodes.begin(), free_nodes.end(), old_placement_info[bid]);
              if (iter!= free_nodes.end()) {
                free_nodes.erase(iter);
              }
            }
          }
          size_t free_nodes_num = free_nodes.size();
          for (int bid : uncommon_bids) {
            int node_idx = random_index(free_nodes_num);
            unsigned int node_id = free_nodes[node_idx];
            new_placement_info[bid] = node_id;
            free_nodes.erase(free_nodes.begin() + node_idx);
            free_nodes_num--;
          }
          auto it = std::find(free_clusters.begin(), free_clusters.end(), cid);
          if (it != free_clusters.end()) {
            free_clusters.erase(it);
          }
        }
      }

      for (int i = 0; i < (int)new_pars.size(); i++) {
        int par_id = common_pars[i];
        if (par_id == -1) {
          // randomly choose a cluster
          int cluster_idx = random_index(free_clusters.size());
          unsigned int cid = free_clusters[cluster_idx];
          int tmp = new_pars[i][0];
          bool flag = false;
          if (tmp < lrc->k + lrc->g) {
            unsigned int t_cid = node_table_[old_placement_info[tmp]].map2cluster;
            auto it = std::find(free_clusters.begin(), free_clusters.end(), t_cid);
            if (it != free_clusters.end()) {
              cid = t_cid;
              flag = true;
            }
          }
          Cluster& cluster = cluster_table_[cid];
          std::vector<unsigned int> free_nodes;
          for (int j = 0; j < num_of_nodes_per_cluster_; j++) {
            free_nodes.push_back(cluster.nodes[j]);
          }
          size_t free_nodes_num = num_of_nodes_per_cluster_;
          for (int bid : new_pars[i]) {
            int node_idx = random_index(free_nodes_num);
            unsigned int node_id = free_nodes[node_idx];
            if (flag && node_table_[old_placement_info[bid]].map2cluster == cid) {
              node_id = old_placement_info[bid];
            }
            new_placement_info[bid] = node_id;
            free_nodes.erase(free_nodes.begin() + node_idx);
            free_nodes_num--;
          }
          auto it = std::find(free_clusters.begin(), free_clusters.end(), cid);
          if (it != free_clusters.end()) {
            free_clusters.erase(it);
          }
        }
      }
    } else {
      std::vector<int> same_pars = find_out_same_partitions(new_pars_copy, old_pars);

      if (IF_DEBUG) {
        std::string msg = "same_pars: ";
        for (auto par_id : same_pars) {
          msg += std::to_string(par_id) + " ";
        }  
        msg += "\n";
        write_logs(Logger::LogLevel::DEBUG, msg);
      }

      std::vector<unsigned int> free_clusters;
      for (unsigned int i = 0; i < num_of_clusters_; i++) {
        free_clusters.push_back(i);
      }

      for (int i = 0; i < (int)new_pars.size(); i++) {
        int par_id = same_pars[i];
        if (par_id != -1) {
          int cid = old_par2cid[par_id];
          for (int bid : new_pars[i]) {
            int old_bid = bid;
            if (bid >= lrc->k + lrc->g && new2old.find(bid)!= new2old.end()) {
              old_bid = new2old.at(bid);
            }
            new_placement_info[bid] = old_placement_info[old_bid];
          }
          auto it = std::find(free_clusters.begin(), free_clusters.end(), cid);
          if (it != free_clusters.end()) {
            free_clusters.erase(it);
          }
        } else {
          // randomly choose a cluster
          int cluster_idx = random_index(free_clusters.size());
          unsigned int cid = free_clusters[cluster_idx];
          int tmp = new_pars[i][0];
          bool flag = false;
          if (tmp < lrc->k + lrc->g) {
            unsigned int t_cid = node_table_[old_placement_info[tmp]].map2cluster;
            auto it = std::find(free_clusters.begin(), free_clusters.end(), t_cid);
            if (it != free_clusters.end()) {
              cid = t_cid;
              flag = true;
            }
          }
          Cluster& cluster = cluster_table_[cid];
          std::vector<unsigned int> free_nodes;
          for (int j = 0; j < num_of_nodes_per_cluster_; j++) {
            free_nodes.push_back(cluster.nodes[j]);
          }
          size_t free_nodes_num = num_of_nodes_per_cluster_;
          for (int bid : new_pars[i]) {
            int node_idx = random_index(free_nodes_num);
            unsigned int node_id = free_nodes[node_idx];
            if (flag && node_table_[old_placement_info[bid]].map2cluster == cid) {
              node_id = old_placement_info[bid];
            }
            new_placement_info[bid] = node_id;
            free_nodes.erase(free_nodes.begin() + node_idx);
            free_nodes_num--;
          }
          auto it = std::find(free_clusters.begin(), free_clusters.end(), cid);
          if (it != free_clusters.end()) {
            free_clusters.erase(it);
          }
        }
      }
    }

    for (auto nid : new_placement_info) {
      my_assert(nid != unreached_node_id);
    }

    return new_placement_info;
  }
}

