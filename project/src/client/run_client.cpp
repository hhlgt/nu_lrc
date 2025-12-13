#include "client.h"
#include <stdio.h>
#include <unistd.h>
#include <fstream>
#include <sstream>
#include <regex>

using namespace ECProject;

void test_single_block_repair(Client &client, int block_num)
{
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::vector<double> repair_times;
  std::vector<double> decoding_times;
  std::vector<double> cross_cluster_times;
  std::vector<double> meta_times;
  std::vector<int> cross_cluster_transfers;
  std::vector<int> io_cnts;
  std::cout << "Single-Block Repair:" << std::endl;
  for (int i = 0; i < stripe_num; i++) {
    std::cout << "[Stripe " << i << "]" << std::endl;
    double temp_repair = 0;
    double temp_decoding = 0;
    double temp_cross_cluster = 0;
    double temp_meta = 0;
    int temp_cc_transfers = 0;
    int temp_io_cnt = 0;
    for (int j = 0; j < block_num; j++) {
      std::vector<unsigned int> failures;
      failures.push_back((unsigned int)j);
      auto resp = client.blocks_repair(failures, stripe_ids[i]);
      temp_repair += resp.repair_time;
      temp_decoding += resp.decoding_time;
      temp_cross_cluster += resp.cross_cluster_time;
      temp_meta += resp.meta_time;
      temp_cc_transfers += resp.cross_cluster_transfers;
      temp_io_cnt += resp.io_cnt;
    }
    repair_times.push_back(temp_repair);
    decoding_times.push_back(temp_decoding);
    cross_cluster_times.push_back(temp_cross_cluster);
    meta_times.push_back(temp_meta);
    cross_cluster_transfers.push_back(temp_cc_transfers);
    io_cnts.push_back(temp_io_cnt);
    std::cout << "repair = " << temp_repair / block_num
              << "s, decoding = " << temp_decoding / block_num
              << "s, cross-cluster = " << temp_cross_cluster / block_num
              << "s, meta = " << temp_meta / block_num
              << "s, cross-cluster-count = " << (double)temp_cc_transfers / block_num
              << ", I/Os = " << temp_io_cnt / block_num
              << std::endl;
  }
  auto avg_repair = std::accumulate(repair_times.begin(),
      repair_times.end(), 0.0) / (stripe_num * block_num);
  auto avg_decoding = std::accumulate(decoding_times.begin(),
      decoding_times.end(), 0.0) / (stripe_num * block_num);
  auto avg_cross_cluster = std::accumulate(cross_cluster_times.begin(),
      cross_cluster_times.end(), 0.0) / (stripe_num * block_num);
  auto avg_meta = std::accumulate(meta_times.begin(),
      meta_times.end(), 0.0) / (stripe_num * block_num);
  auto avg_cc_transfers = (double)std::accumulate(cross_cluster_transfers.begin(),
      cross_cluster_transfers.end(), 0) / (stripe_num * block_num);
  auto avg_io_cnt = (double)std::accumulate(io_cnts.begin(),
      io_cnts.end(), 0) / (stripe_num * block_num);
  std::cout << "^-^[Average]^-^" << std::endl;
  std::cout << "repair = " << avg_repair << "s, decoding = " << avg_decoding
            << "s, cross-cluster = " << avg_cross_cluster
            << "s, meta = " << avg_meta
            << "s, cross-cluster-count = " << avg_cc_transfers
            << ", I/Os = " << avg_io_cnt
            << std::endl;
}

void test_multiple_blocks_repair(Client &client, int block_num, const ParametersInfo& paras)
{
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::vector<double> repair_times;
  std::vector<double> decoding_times;
  std::vector<double> cross_cluster_times;
  std::vector<double> meta_times;
  std::vector<int> cross_cluster_transfers;
  std::vector<int> io_cnts;
  int run_time = 5;
  int tot_cnt = 0;
  LocallyRepairableCode* lrc = lrc_factory(paras.ec_type, paras.cp);
  std::cout << "Multi-Block Repair:" << std::endl;
  for (int i = 0; i < stripe_num; i++) {
    std::cout << "[Stripe " << i << "]" << std::endl;
    double temp_repair = 0;
    double temp_decoding = 0;
    double temp_cross_cluster = 0;
    double temp_meta = 0;
    int temp_cc_transfers = 0;
    int temp_io_cnt = 0;
    int cnt = 0;
    for (int j = 0; j < run_time; j++) {
      int failed_num = random_range(2, 4);
      std::vector<int> failed_blocks;
      random_n_num(0, block_num - 1, failed_num, failed_blocks);
      std::vector<unsigned int> failures;
      for (auto& block : failed_blocks) {
        failures.push_back((unsigned int)block);
      }
      if (!lrc->check_if_decodable(failed_blocks)) {
        j--;
        continue;
      }
      auto resp = client.blocks_repair(failures, stripe_ids[i]);
      if (resp.success) {
        temp_repair += resp.repair_time;
        temp_decoding += resp.decoding_time;
        temp_cross_cluster += resp.cross_cluster_time;
        temp_meta += resp.meta_time;
        temp_cc_transfers += resp.cross_cluster_transfers;
        temp_io_cnt += resp.io_cnt;
        cnt++;
      }
    }
    repair_times.push_back(temp_repair);
    decoding_times.push_back(temp_decoding);
    cross_cluster_times.push_back(temp_cross_cluster);
    meta_times.push_back(temp_meta);
    cross_cluster_transfers.push_back(temp_cc_transfers);
    io_cnts.push_back(temp_io_cnt);
    std::cout << "repair = " << temp_repair / cnt
              << "s, decoding = " << temp_decoding / cnt
              << "s, cross-cluster = " << temp_cross_cluster / cnt
              << "s, meta = " << temp_meta / cnt
              << "s, cross-cluster-count = " << (double)temp_cc_transfers / cnt
              << ", I/Os = " << temp_io_cnt / cnt
              << std::endl;
    tot_cnt += cnt;
  }
  auto avg_repair = std::accumulate(repair_times.begin(),
      repair_times.end(), 0.0) / tot_cnt;
  auto avg_decoding = std::accumulate(decoding_times.begin(),
      decoding_times.end(), 0.0) / tot_cnt;
  auto avg_cross_cluster = std::accumulate(cross_cluster_times.begin(),
      cross_cluster_times.end(), 0.0) / tot_cnt;
  auto avg_meta = std::accumulate(meta_times.begin(),
      meta_times.end(), 0.0) / tot_cnt;
  auto avg_cc_transfers = (double)std::accumulate(cross_cluster_transfers.begin(),
      cross_cluster_transfers.end(), 0) / tot_cnt;
  auto avg_io_cnt = (double)std::accumulate(io_cnts.begin(),
      io_cnts.end(), 0) / tot_cnt;
  std::cout << "^-^[Average]^-^" << std::endl;
  std::cout << "repair = " << avg_repair << "s, decoding = " << avg_decoding
            << "s, cross-cluster = " << avg_cross_cluster
            << "s, meta = " << avg_meta
            << "s, cross-cluster-count = " << avg_cc_transfers
            << ", I/Os = " << avg_io_cnt
            << std::endl;
  if (lrc != nullptr) {
    delete lrc;
    lrc = nullptr;
  }
}

void test_multiple_blocks_repair_lrc(Client &client, const ParametersInfo& paras,
    int failed_num)
{
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::vector<double> repair_times;
  std::vector<double> decoding_times;
  std::vector<double> cross_cluster_times;
  std::vector<double> meta_times;
  std::vector<int> cross_cluster_transfers;
  std::vector<int> io_cnts;
  int run_time = 10;
  int tot_cnt = 0;
  LocallyRepairableCode* lrc = lrc_factory(paras.ec_type, paras.cp);
  std::vector<std::vector<int>> groups;
  lrc->grouping_information(groups);
  int group_num = (int)groups.size();
  std::cout << "Multi-Block Repair:" << std::endl;
  for (int i = 0; i < stripe_num; i++) {
    std::cout << "[Stripe " << i << "]" << std::endl;
    double temp_repair = 0;
    double temp_decoding = 0;
    double temp_cross_cluster = 0;
    double temp_meta = 0;
    int temp_cc_transfers = 0;
    int temp_io_cnt = 0;
    int cnt = 0;
    for (int j = 0; j < run_time; j++) {
      // int gid = random_index((size_t)group_num);
      int ran_data_idx = random_index((size_t)(lrc->k + lrc->g));
      int gid = ran_data_idx / lrc->r;
      std::vector<int> failed_blocks;
      random_n_element(2, groups[gid], failed_blocks);
      if (failed_num > 2) {
        int t_gid = random_index((size_t)group_num);
        int t_idx = random_index(groups[t_gid].size());
        int failed_idx = groups[t_gid][t_idx];
        while (std::find(failed_blocks.begin(), failed_blocks.end(), failed_idx)
            != failed_blocks.end()) {
          t_gid = random_index((size_t)group_num);
          t_idx = random_index(groups[t_gid].size());
          failed_idx = groups[t_gid][t_idx];
        }
        failed_blocks.push_back(failed_idx);
        if (failed_num > 3) {
          int tt_gid = 0;
          if (gid == t_gid && paras.cp.g < 3) {
            tt_gid = (gid + random_index((size_t)(group_num - 1)) + 1) % group_num;
          } else {
            tt_gid = random_index((size_t)group_num);
          }
          t_idx = random_index(groups[tt_gid].size());
          failed_idx = groups[tt_gid][t_idx];
          while (std::find(failed_blocks.begin(), failed_blocks.end(), failed_idx)
            != failed_blocks.end()) {
            if (gid == t_gid && paras.cp.g < 3) {
              tt_gid = (gid + random_index((size_t)(group_num - 1)) + 1) % group_num;
            } else {
              tt_gid = random_index((size_t)group_num);
            }
            t_idx = random_index(groups[tt_gid].size());
            failed_idx = groups[tt_gid][t_idx];
          }
          failed_blocks.push_back(failed_idx);
        }
      }
      if (!lrc->check_if_decodable(failed_blocks)) {
        j--;
        continue;
      }
      std::vector<unsigned int> failures;
      for (auto& block : failed_blocks) {
        failures.push_back((unsigned int)block);
      }
      auto resp = client.blocks_repair(failures, stripe_ids[i]);
      if (resp.success) {
        temp_repair += resp.repair_time;
        temp_decoding += resp.decoding_time;
        temp_cross_cluster += resp.cross_cluster_time;
        temp_meta += resp.meta_time;
        temp_cc_transfers += resp.cross_cluster_transfers;
        temp_io_cnt += resp.io_cnt;
        cnt++;
      }
    }
    repair_times.push_back(temp_repair);
    decoding_times.push_back(temp_decoding);
    cross_cluster_times.push_back(temp_cross_cluster);
    meta_times.push_back(temp_meta);
    cross_cluster_transfers.push_back(temp_cc_transfers);
    io_cnts.push_back(temp_io_cnt);
    std::cout << "repair = " << temp_repair / cnt
              << "s, decoding = " << temp_decoding / cnt
              << "s, cross-cluster = " << temp_cross_cluster / cnt
              << "s, meta = " << temp_meta / cnt
              << "s, cross-cluster-count = " << (double)temp_cc_transfers / cnt
              << ", I/Os = " << temp_io_cnt / cnt
              << std::endl;
    tot_cnt += cnt;
  }
  auto avg_repair = std::accumulate(repair_times.begin(),
      repair_times.end(), 0.0) / tot_cnt;
  auto avg_decoding = std::accumulate(decoding_times.begin(),
      decoding_times.end(), 0.0) / tot_cnt;
  auto avg_cross_cluster = std::accumulate(cross_cluster_times.begin(),
      cross_cluster_times.end(), 0.0) / tot_cnt;
  auto avg_meta = std::accumulate(meta_times.begin(),
      meta_times.end(), 0.0) / tot_cnt;
  auto avg_cc_transfers = (double)std::accumulate(cross_cluster_transfers.begin(),
      cross_cluster_transfers.end(), 0) / tot_cnt;
  auto avg_io_cnt = (double)std::accumulate(io_cnts.begin(),
      io_cnts.end(), 0) / tot_cnt;
  std::cout << "^-^[Average]^-^" << std::endl;
  std::cout << "repair = " << avg_repair << "s, decoding = " << avg_decoding
            << "s, cross-cluster = " << avg_cross_cluster
            << "s, meta = " << avg_meta
            << "s, cross-cluster-count = " << avg_cc_transfers
            << ", I/Os = " << avg_io_cnt
            << std::endl;
  if (lrc != nullptr) {
    delete lrc;
    lrc = nullptr;
  }
}

void test_stripe_merging(Client &client, int step_size)
{
  my_assert(step_size > 1);
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::cout << "Stripe Merging:" << std::endl;
  auto resp = client.merge(step_size);
  std::cout << "[Total]" << std::endl;
  std::cout << "merging = " << resp.merging_time
            << "s, computing = " << resp.computing_time
            << "s, cross-cluster = " << resp.cross_cluster_time
            << "s, meta =" << resp.meta_time
            << "s, cross-cluster-count = " << resp.cross_cluster_transfers
            << ", I/Os = " << resp.io_cnt
            << std::endl;
  std::cout << "[Average for every " << step_size << " stripes]" << std::endl;
  std::cout << "merging = " << resp.merging_time / stripe_num
            << "s, computing = " << resp.computing_time / stripe_num
            << "s, cross-cluster = " << resp.cross_cluster_time / stripe_num
            << "s, meta =" << resp.meta_time / stripe_num
            << "s, cross-cluster-count = "
            << (double)resp.cross_cluster_transfers / stripe_num
            << ", I/Os = " << resp.io_cnt / stripe_num
            << std::endl;
}

void generate_random_multi_block_failures_lrc(std::string filename,
    int stripe_num, const ParametersInfo& paras, int failed_num,
    const std::vector<std::vector<size_t>> &ms_object_sizes,
    const std::vector<std::vector<unsigned int>> &ms_object_accessrates)
{
  LocallyRepairableCode* lrc = lrc_factory(paras.ec_type, paras.cp);
  std::string suf = lrc->type() + "_" + std::to_string(paras.cp.k) + "_" + \
      std::to_string(paras.cp.l) + "_" + std::to_string(paras.cp.g) + "_" + \
      std::to_string(failed_num);
  filename += suf;
  std::ofstream outFile(filename);
  if (!outFile) {
    std::cerr << "Error! Unable to open " << filename << std::endl;
    return;
  }
  int cases_per_stripe = 10;
  for (int i = 0; i < stripe_num; i++) {
    auto nu_lrc = dynamic_cast<Non_Uni_LRC*>(lrc);
    nu_lrc->storage_overhead = float(paras.cp.k + paras.cp.g + paras.cp.l) / float(paras.cp.k);
    nu_lrc->generate_coding_parameters_for_a_stripe(ms_object_sizes[i], ms_object_accessrates[i], paras.block_size);
    std::vector<std::vector<int>> groups;
    nu_lrc->generate_groups_info();
    nu_lrc->grouping_information(groups);
    int group_num = (int)groups.size();
    std::cout << nu_lrc->self_information() << std::endl;
    for (auto& group : groups) {
      for (auto bid : group) {
        std::cout << bid << " ";
      }
      std::cout << "\n";
    }
    for (int ii = 0; ii < cases_per_stripe; ii++) {
      // int ran_data_idx = random_index((size_t)(lrc->k + lrc->g));
      // int gid = lrc->bid2gid(ran_data_idx);
      int gid = random_index(group_num);
      std::vector<int> failed_blocks;
      random_n_element(2, groups[gid], failed_blocks);
      if (failed_num > 2) {
        int t_gid = random_index((size_t)group_num);
        int t_idx = random_index(groups[t_gid].size());
        int failed_idx = groups[t_gid][t_idx];
        while (std::find(failed_blocks.begin(), failed_blocks.end(), failed_idx)
            != failed_blocks.end()) {
          t_gid = random_index((size_t)group_num);
          t_idx = random_index(groups[t_gid].size());
          failed_idx = groups[t_gid][t_idx];
        }
        failed_blocks.push_back(failed_idx);
        if (failed_num > 3) {
          int tt_gid = 0;
          if (gid == t_gid && paras.cp.g < 3) {
            tt_gid = (gid + random_index((size_t)(group_num - 1)) + 1) % group_num;
          } else {
            tt_gid = random_index((size_t)group_num);
          }
          my_assert(tt_gid < group_num);
          t_idx = random_index(groups[tt_gid].size());
          my_assert(t_idx < groups[tt_gid].size());
          failed_idx = groups[tt_gid][t_idx];
          while (std::find(failed_blocks.begin(), failed_blocks.end(), failed_idx)
            != failed_blocks.end()) {
            if (gid == t_gid && paras.cp.g < 3) {
              tt_gid = (gid + random_index((size_t)(group_num - 1)) + 1) % group_num;
            } else {
              tt_gid = random_index((size_t)group_num);
            }
            my_assert(tt_gid < group_num);
            t_idx = random_index(groups[tt_gid].size());
            my_assert(t_idx < groups[tt_gid].size());
            failed_idx = groups[tt_gid][t_idx];
          }
          failed_blocks.push_back(failed_idx);
        }
      }
      if (!nu_lrc->check_if_decodable(failed_blocks)) {
        ii--;
        continue;
      } else {
        outFile << i << " ";
        for (const auto& num : failed_blocks) {
          outFile << num << " ";
        }
        outFile << "\n";
      }
    }
  }
  outFile.close();
  if (lrc != nullptr) {
    delete lrc;
    lrc = nullptr;
  }
}

void test_multiple_blocks_repair_lrc_with_testcases(std::string filename,
    Client &client, const ParametersInfo& paras, int failed_num)
{
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::vector<double> repair_times;
  std::vector<double> decoding_times;
  std::vector<double> cross_cluster_times;
  std::vector<double> meta_times;
  std::vector<int> cross_cluster_transfers;
  std::vector<int> io_cnts;
  int run_time = 10;
  int tot_cnt = 0;
  LocallyRepairableCode* lrc = lrc_factory(paras.ec_type, paras.cp);
  std::vector<std::vector<int>> groups;
  lrc->grouping_information(groups);
  int group_num = (int)groups.size();
  std::string suf = lrc->type() + "_" + std::to_string(paras.cp.k) + "_" + \
      std::to_string(paras.cp.l) + "_" + std::to_string(paras.cp.g) + "_" + \
      std::to_string(failed_num);
  filename += suf;
  std::ifstream inFile(filename);
  if (!inFile) {
    std::cerr << "Error! Unable to open " << filename << std::endl;
    return;
  }
  std::string line;
  std::cout << "Multi-Block Repair:" << std::endl;
  for (int i = 0; i < stripe_num; i++) {
    std::cout << "[Stripe " << i << "]" << std::endl;
    double temp_repair = 0;
    double temp_decoding = 0;
    double temp_cross_cluster = 0;
    double temp_meta = 0;
    int temp_cc_transfers = 0;
    int temp_io_cnt = 0;
    int cnt = 0;
    for (int j = 0; j < run_time; j++) {
      std::vector<int> failed_blocks;
      std::getline(inFile, line);
      std::istringstream lineStream(line);
      int sid;
      lineStream >> sid;
      int num;
      while (lineStream >> num) {
        failed_blocks.push_back(num);
      }
      std::vector<unsigned int> failures;
      for (auto& block : failed_blocks) {
        failures.push_back((unsigned int)block);
      }
      auto resp = client.blocks_repair(failures, sid);
      if (resp.success) {
        temp_repair += resp.repair_time;
        temp_decoding += resp.decoding_time;
        temp_cross_cluster += resp.cross_cluster_time;
        temp_meta += resp.meta_time;
        temp_cc_transfers += resp.cross_cluster_transfers;
        temp_io_cnt += resp.io_cnt;
        cnt++;
      }
    }
    repair_times.push_back(temp_repair);
    decoding_times.push_back(temp_decoding);
    cross_cluster_times.push_back(temp_cross_cluster);
    meta_times.push_back(temp_meta);
    cross_cluster_transfers.push_back(temp_cc_transfers);
    io_cnts.push_back(temp_io_cnt);
    std::cout << "repair = " << temp_repair / cnt
              << "s, decoding = " << temp_decoding / cnt
              << "s, cross-cluster = " << temp_cross_cluster / cnt
              << "s, meta = " << temp_meta / cnt
              << "s, cross-cluster-count = " << (double)temp_cc_transfers / cnt
              << ", I/Os = " << temp_io_cnt / cnt
              << std::endl;
    tot_cnt += cnt;
  }
  inFile.close();
  auto avg_repair = std::accumulate(repair_times.begin(),
      repair_times.end(), 0.0) / tot_cnt;
  auto avg_decoding = std::accumulate(decoding_times.begin(),
      decoding_times.end(), 0.0) / tot_cnt;
  auto avg_cross_cluster = std::accumulate(cross_cluster_times.begin(),
      cross_cluster_times.end(), 0.0) / tot_cnt;
  auto avg_meta = std::accumulate(meta_times.begin(),
      meta_times.end(), 0.0) / tot_cnt;
  auto avg_cc_transfers = (double)std::accumulate(cross_cluster_transfers.begin(),
      cross_cluster_transfers.end(), 0) / tot_cnt;
  auto avg_io_cnt = (double)std::accumulate(io_cnts.begin(),
      io_cnts.end(), 0) / tot_cnt;
  std::cout << "^-^[Average]^-^" << std::endl;
  std::cout << "repair = " << avg_repair << "s, decoding = " << avg_decoding
            << "s, cross-cluster = " << avg_cross_cluster
            << "s, meta = " << avg_meta
            << "s, cross-cluster-count = " << avg_cc_transfers
            << ", I/Os = " << avg_io_cnt
            << std::endl;
  if (lrc != nullptr) {
    delete lrc;
    lrc = nullptr;
  }
}

void parse_tracefile(std::string tracefile_path, int block_size,
        std::vector<float> &ms_storage_overhead, std::vector<int> &ms_g, 
        std::vector<std::vector<size_t>> &ms_object_sizes,
        std::vector<std::vector<unsigned int>> &ms_object_accessrates)
{
    std::ifstream file(tracefile_path);
    if (file.is_open()) {
        std::string line;
        while (std::getline(file, line)) {
            std::vector<size_t> object_sizes;
            std::vector<unsigned int> object_accessrates;
            std::regex pattern1("(\\d+\\.\\d+),(\\d+)");
            std::sregex_iterator it1(line.begin(), line.end(), pattern1);
            std::sregex_iterator end1;
            while (it1 != end1) {
                std::smatch match = *it1;
                ms_storage_overhead.push_back(std::stof(match[1].str()));
                ms_g.push_back(std::stoi(match[2].str()));
                ++it1;
            }
            std::regex pattern2("\\((\\d+),(\\d+)\\)");
            std::sregex_iterator it2(line.begin(), line.end(), pattern2);
            std::sregex_iterator end2;
            while (it2 != end2) {
                std::smatch match = *it2;
                object_sizes.push_back((size_t)(std::stoi(match[1].str()) * block_size));
                object_accessrates.push_back((unsigned int)std::stoi(match[2].str()));
                ++it2;
            }
            ms_object_sizes.push_back(object_sizes);
            ms_object_accessrates.push_back(object_accessrates);
        }
        file.close();
    } else {
        std::cerr << "Failed to open the file." << std::endl;
    }
}

void test_single_block_repair_lrc_periodically(Client &client,
    const std::vector<std::vector<size_t>> &ms_object_sizes,
    const std::vector<std::vector<unsigned int>> &ms_object_accessrates,
    const std::vector<float> ms_storge_overhead,
    const std::vector<int> ms_g,
    size_t block_size, bool if_node_repair = false)
{
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::vector<double> repair_times;
  std::vector<double> decoding_times;
  std::vector<double> cross_cluster_times;
  std::vector<double> meta_times;
  std::vector<int> cross_cluster_transfers;
  std::vector<int> io_cnts;
  int run_time = 5;
  int tot_cnt = 0;
  std::cout << "Single-Block Repair:" << std::endl;
  for (int i = 0; i < run_time; i++) {
    std::cout << "[rumtime " << i << "]" << std::endl;
    double temp_repair = 0;
    double temp_decoding = 0;
    double temp_cross_cluster = 0;
    double temp_meta = 0;
    int temp_cc_transfers = 0;
    int temp_io_cnt = 0;
    int cnt = 0;
    for (int j = 0; j < stripe_num; j++) {
      unsigned int sid = stripe_ids[j];
      int k = 0;
      int ak = 0;
      int idx = 0;
      for (auto& size : ms_object_sizes[sid]) {
        k += size / block_size;
        ak += (size / block_size) * ms_object_accessrates[sid][idx++];
      }
      int a_avg = ak / k;
      int l = int(round(ms_storge_overhead[sid] * k)) - k - ms_g[sid];
      int r = (k + ms_g[sid] + l - 1) / l;
      l = (k + ms_g[sid] + r - 1) / r;

      int n_files = int(ms_object_sizes[sid].size());
      std::vector<size_t> object_sizes = ms_object_sizes[sid];
      std::vector<unsigned int> object_accessrates = ms_object_accessrates[sid];
      int lowerbound = 0, upperbound = 0;
      for(int ii = 0; ii < n_files; ii++)
      {
        int obj_len = object_sizes[ii] / block_size;
        upperbound += obj_len;
        for(int jj = 0; jj < object_accessrates[ii]; jj++) { // proportion to access frequency
          // int ran_num = random_index(100);
          // if (ran_num < 20) {
          
          int ran_fblock_id = random_index(k);  // each data block has the same probability of being failed
          if(ran_fblock_id >= lowerbound && ran_fblock_id < upperbound) {// a block in requested file failed
            std::vector<unsigned int> failures;
            failures.push_back((unsigned int)ran_fblock_id);
            auto resp = client.blocks_repair(failures, sid);
            if (resp.success) {
              temp_repair += resp.repair_time;
              temp_decoding += resp.decoding_time;
              temp_cross_cluster += resp.cross_cluster_time;
              temp_meta += resp.meta_time;
              temp_cc_transfers += resp.cross_cluster_transfers;
              temp_io_cnt += resp.io_cnt;
              cnt++;
            }
          }
          
          // }
        }
        
        // int ar = object_accessrates[ii];
        // for(int jj = lowerbound; jj < upperbound; jj++)
        // {
        //   std::vector<unsigned int> failures;
        //   failures.push_back(jj);
        //   auto resp = client.blocks_repair(failures, sid);
        //   if (resp.success) {
        //     temp_repair += resp.repair_time * ar;
        //     temp_decoding += resp.decoding_time * ar;
        //     temp_cross_cluster += resp.cross_cluster_time * ar;
        //     temp_meta += resp.meta_time * ar;
        //     temp_cc_transfers += resp.cross_cluster_transfers * ar;
        //     temp_io_cnt += resp.io_cnt * ar;
        //     cnt += ar;
        //   }
        // }

        lowerbound += obj_len;
      }
      if (if_node_repair) {
        for (auto ii = k; ii < k + ms_g[sid] + l; ii++) {
          std::vector<unsigned int> failures;
          failures.push_back((unsigned int)ii);
          auto resp = client.blocks_repair(failures, sid);
          if (resp.success) {
            temp_repair += resp.repair_time * a_avg;
            temp_decoding += resp.decoding_time * a_avg;
            temp_cross_cluster += resp.cross_cluster_time * a_avg;
            temp_meta += resp.meta_time * a_avg;
            temp_cc_transfers += resp.cross_cluster_transfers * a_avg;
            temp_io_cnt += resp.io_cnt * a_avg;
            cnt += a_avg;
          }
        }
      }
    }
    repair_times.push_back(temp_repair);
    decoding_times.push_back(temp_decoding);
    cross_cluster_times.push_back(temp_cross_cluster);
    meta_times.push_back(temp_meta);
    cross_cluster_transfers.push_back(temp_cc_transfers);
    io_cnts.push_back(temp_io_cnt);
    std::cout << "repair = " << temp_repair / cnt
              << "s, decoding = " << temp_decoding / cnt
              << "s, cross-cluster = " << temp_cross_cluster / cnt
              << "s, meta = " << temp_meta / cnt
              << "s, cross-cluster-count = " << (double)temp_cc_transfers / cnt
              << ", I/Os = " << temp_io_cnt / cnt
              << std::endl;
    tot_cnt += cnt;
  }
  auto avg_repair = std::accumulate(repair_times.begin(),
      repair_times.end(), 0.0) / tot_cnt;
  auto avg_decoding = std::accumulate(decoding_times.begin(),
      decoding_times.end(), 0.0) / tot_cnt;
  auto avg_cross_cluster = std::accumulate(cross_cluster_times.begin(),
      cross_cluster_times.end(), 0.0) / tot_cnt;
  auto avg_meta = std::accumulate(meta_times.begin(),
      meta_times.end(), 0.0) / tot_cnt;
  auto avg_cc_transfers = (double)std::accumulate(cross_cluster_transfers.begin(),
      cross_cluster_transfers.end(), 0) / tot_cnt;
  auto avg_io_cnt = (double)std::accumulate(io_cnts.begin(),
      io_cnts.end(), 0) / tot_cnt;
  std::cout << "^-^[Average]^-^" << std::endl;
  std::cout << "repair = " << avg_repair << "s, decoding = " << avg_decoding
            << "s, cross-cluster = " << avg_cross_cluster
            << "s, meta = " << avg_meta
            << "s, cross-cluster-count = " << avg_cc_transfers
            << ", I/Os = " << avg_io_cnt
            << std::endl;
}

void test_multi_blocks_repair_lrc_periodically(Client &client,
    const std::vector<std::vector<size_t>> &ms_object_sizes,
    const std::vector<std::vector<unsigned int>> &ms_object_accessrates,
    const std::vector<float> ms_storge_overhead,
    const std::vector<int> ms_g,
    size_t block_size)
{
  auto stripe_ids = client.list_stripes();
  int stripe_num = stripe_ids.size();
  std::vector<double> repair_times;
  std::vector<double> decoding_times;
  std::vector<double> cross_cluster_times;
  std::vector<double> meta_times;
  std::vector<int> cross_cluster_transfers;
  std::vector<int> io_cnts;
  int run_time = 5;
  int tot_cnt = 0;
  std::cout << "Two-Block Repair:" << std::endl;
  for (int i = 0; i < run_time; i++) {
    std::cout << "[rumtime " << i << "]" << std::endl;
    double temp_repair = 0;
    double temp_decoding = 0;
    double temp_cross_cluster = 0;
    double temp_meta = 0;
    int temp_cc_transfers = 0;
    int temp_io_cnt = 0;
    int cnt = 0;
    for (int j = 0; j < stripe_num; j++) {
      unsigned int sid = stripe_ids[j];
      int k = 0;
      int ak = 0;
      int idx = 0;
      int bid = 0;
      std::unordered_map<int, int> b2f;
      for (auto& size : ms_object_sizes[sid]) {
        int ki = size / block_size;
        k += ki;
        ak += ki * ms_object_accessrates[sid][idx];
        for (int ii = 0; ii < ki; ii++) {
          b2f[bid++] = idx;
        }
        ++idx;
      }
      int a_avg = ak / k;
      int l = int(round(ms_storge_overhead[sid] * k)) - k - ms_g[sid];
      int r = (k + ms_g[sid] + l - 1) / l;
      l = (k + ms_g[sid] + r - 1) / r;

      int n_files = int(ms_object_sizes[sid].size());
      std::vector<size_t> object_sizes = ms_object_sizes[sid];
      std::vector<unsigned int> object_accessrates = ms_object_accessrates[sid];
      int lowerbound = 0, upperbound = 0;
      for(int ii = 0; ii < n_files; ii++)
      {
        int file_len = object_sizes[ii] / block_size;
        upperbound += file_len;
        for(int jj = 0; jj < object_accessrates[ii]; jj++) { // proportion to access frequency
          int ran_num = random_index(100);
          if (ran_num < 20) {
          
          std::vector<int> ran_fblock_ids;
          random_n_num(0, k - 1, 2, ran_fblock_ids);
          bool flag = false;
          for(int ran_fblock_id : ran_fblock_ids) { // each data block has the same probability of being failed
            if(ran_fblock_id >= lowerbound && ran_fblock_id < upperbound) {// a block in requested file failed
              flag = true;
              break;
            }
          }
          if(flag) {// a block in requested file failed 
            std::vector<unsigned int> failures;
            for (auto& id : ran_fblock_ids) {
              if (id >= lowerbound && id < upperbound) {
                failures.push_back((unsigned int)id); 
              }
            }
            auto resp = client.blocks_repair(failures, sid);
            if (resp.success) {
              temp_repair += resp.repair_time;
              temp_decoding += resp.decoding_time;
              temp_cross_cluster += resp.cross_cluster_time;
              temp_meta += resp.meta_time;
              temp_cc_transfers += resp.cross_cluster_transfers;
              temp_io_cnt += resp.io_cnt;
              cnt++;
            }
          }

          }
        }
        lowerbound += file_len;
      }

      // for (int ii = 0; ii < k; ii++) {
      //   for (int jj = ii + 1; jj < k; jj++) {
      //     int ran_num = random_index(100);
      //     if (ran_num < 20) {
      //       std::vector<unsigned int> failures;
      //       failures.push_back((unsigned int)ii);
      //       failures.push_back((unsigned int)jj);
      //       int weight = (ms_object_accessrates[sid][b2f[ii]] + ms_object_accessrates[sid][b2f[jj]]) / 2;
      //       auto resp = client.blocks_repair(failures, sid);
      //       if (resp.success) {
      //         temp_repair += resp.repair_time * weight;
      //         temp_decoding += resp.decoding_time * weight;
      //         temp_cross_cluster += resp.cross_cluster_time * weight;
      //         temp_meta += resp.meta_time * weight;
      //         temp_cc_transfers += resp.cross_cluster_transfers * weight;
      //         temp_io_cnt += resp.io_cnt * weight;
      //         cnt += weight;
      //       }
      //     }
      //   }
      // }
    }
    repair_times.push_back(temp_repair);
    decoding_times.push_back(temp_decoding);
    cross_cluster_times.push_back(temp_cross_cluster);
    meta_times.push_back(temp_meta);
    cross_cluster_transfers.push_back(temp_cc_transfers);
    io_cnts.push_back(temp_io_cnt);
    std::cout << "repair = " << temp_repair / cnt
              << "s, decoding = " << temp_decoding / cnt
              << "s, cross-cluster = " << temp_cross_cluster / cnt
              << "s, meta = " << temp_meta / cnt
              << "s, cross-cluster-count = " << (double)temp_cc_transfers / cnt
              << ", I/Os = " << temp_io_cnt / cnt
              << std::endl;
    tot_cnt += cnt;
  }
  auto avg_repair = std::accumulate(repair_times.begin(),
      repair_times.end(), 0.0) / tot_cnt;
  auto avg_decoding = std::accumulate(decoding_times.begin(),
      decoding_times.end(), 0.0) / tot_cnt;
  auto avg_cross_cluster = std::accumulate(cross_cluster_times.begin(),
      cross_cluster_times.end(), 0.0) / tot_cnt;
  auto avg_meta = std::accumulate(meta_times.begin(),
      meta_times.end(), 0.0) / tot_cnt;
  auto avg_cc_transfers = (double)std::accumulate(cross_cluster_transfers.begin(),
      cross_cluster_transfers.end(), 0) / tot_cnt;
  auto avg_io_cnt = (double)std::accumulate(io_cnts.begin(),
      io_cnts.end(), 0) / tot_cnt;
  std::cout << "^-^[Average]^-^" << std::endl;
  std::cout << "repair = " << avg_repair << "s, decoding = " << avg_decoding
            << "s, cross-cluster = " << avg_cross_cluster
            << "s, meta = " << avg_meta
            << "s, cross-cluster-count = " << avg_cc_transfers
            << ", I/Os = " << avg_io_cnt
            << std::endl;
}

struct WorkFlows
{
  std::vector<std::vector<size_t>> ms_object_sizes;
  std::vector<std::vector<std::vector<unsigned int>>> ms_object_accessrates;
  std::vector<float> ms_storge_overhead;
  std::vector<int> ms_g;
};

void save_workflows_to_file(const std::string& file_path, size_t block_size,
        const WorkFlows& workflows)
{
  std::ofstream outfile(file_path);
  if (!outfile.is_open()) {
    std::cerr << "Failed to open output file: " << file_path << std::endl;
    return;
  }
  for (size_t time_point = 0; time_point < workflows.ms_object_accessrates.size(); ++time_point) {
    for (size_t stripe_idx = 0; stripe_idx < workflows.ms_object_sizes.size(); ++stripe_idx) {
      outfile << time_point << "," 
              << workflows.ms_storge_overhead[stripe_idx] << "," 
              << workflows.ms_g[stripe_idx];

      const auto& sizes = workflows.ms_object_sizes[stripe_idx];
      const auto& access_rates = workflows.ms_object_accessrates[time_point][stripe_idx];
            
      for (size_t obj_idx = 0; obj_idx < sizes.size(); ++obj_idx) {
        outfile << ",(" << sizes[obj_idx] / block_size << "," << access_rates[obj_idx] << ")";
      }
      outfile << std::endl;
    }
  }
  outfile.close();
}

void parse_tracefile_workflows(std::string tracefile_path, size_t block_size,
                     WorkFlows& workflows)
{
  std::ifstream file(tracefile_path);
  if (file.is_open()) {
    std::string line;
    while (std::getline(file, line)) {
      std::vector<size_t> object_sizes;
      std::vector<unsigned int> object_accessrates;
      std::regex pattern1("(\\d+),(\\d+\\.\\d+),(\\d+)");
      std::sregex_iterator it1(line.begin(), line.end(), pattern1);
      std::sregex_iterator end1;
      int time_point = 0;
      while (it1 != end1) {
        std::smatch match = *it1;
        time_point = std::stoi(match[1].str());
        if (time_point == 0) {
          workflows.ms_storge_overhead.emplace_back(std::stof(match[2].str()));
          workflows.ms_g.emplace_back(std::stoi(match[3].str()));
        }
        ++it1;
      }
      std::regex pattern2("\\((\\d+),(\\d+)\\)");
      std::sregex_iterator it2(line.begin(), line.end(), pattern2);
      std::sregex_iterator end2;
      while (it2 != end2) {
        std::smatch match = *it2;
        object_sizes.emplace_back((size_t)(std::stoi(match[1].str()) * block_size));
        object_accessrates.emplace_back((unsigned int)std::stoi(match[2].str()));
        ++it2;
      }
      if (time_point == 0) {
        workflows.ms_object_sizes.emplace_back(object_sizes);
      }
      if (time_point == workflows.ms_object_accessrates.size()) {
        workflows.ms_object_accessrates.resize(time_point + 1);
      }
      workflows.ms_object_accessrates[time_point].emplace_back(object_accessrates);
    }
    file.close();
  } else {
    std::cerr << "Failed to open the file." << std::endl;
  }
}

void generate_workflows(WorkFlows& workflows, size_t block_size, std::string tracefile_path,
    const ScaleParameters& scale_paras, bool from_file = false)
{
  std::string save_file_path;
  size_t last_dot_pos = tracefile_path.find_last_of(".");
  size_t last_slash_pos = tracefile_path.find_last_of("/");
  size_t last_backslash_pos = tracefile_path.find_last_of("\\");
  size_t last_path_separator = std::max(last_slash_pos, last_backslash_pos);
  if (last_dot_pos == std::string::npos || last_dot_pos < last_path_separator) {
    save_file_path = tracefile_path.substr(0, last_dot_pos) + "_workflows" + ".txt";
  } else {
    save_file_path = tracefile_path.substr(0, last_dot_pos) + "_workflow" + tracefile_path.substr(last_dot_pos);
  }

  if (!from_file) {
    workflows.ms_object_accessrates.resize(scale_paras.test_time + 1);
    parse_tracefile(tracefile_path, block_size, workflows.ms_storge_overhead, workflows.ms_g,
        workflows.ms_object_sizes, workflows.ms_object_accessrates[0]);
    for (int i = 1; i <= scale_paras.test_time; i++) {
      workflows.ms_object_accessrates[i] = workflows.ms_object_accessrates[i - 1];
      double change_rate = scale_paras.change_rate;
      for (auto& object_accessrates : workflows.ms_object_accessrates[i]) {
        for (auto& accessrate : object_accessrates) {
          int ran_1000 = random_index(1000);
          if (double(ran_1000) / 1000.0 < change_rate) {
            int ran_200 = random_index(200);
            double randomFactor = 1.0 + ((ran_200 - 100) / 100.0);
            accessrate = static_cast<unsigned int>(accessrate * randomFactor);
            if (accessrate == 0) accessrate = 1;
          }
        }
      }
    }
    save_workflows_to_file(save_file_path, block_size, workflows);
  } else {
    parse_tracefile_workflows(save_file_path, block_size, workflows);
    my_assert(workflows.ms_object_accessrates.size() == scale_paras.test_time + 1);
  }
}

void test_repair_performance_periodically_v2(
    std::string path_prefix, std::string tracefile,
    int stripe_num, ParametersInfo& paras,
    ScaleParameters& scale_paras, int failed_num,
    bool from_file = false, int startcase = 0)
{
  std::string tracefile_path = path_prefix + "/../../tracefile/" + tracefile;
  WorkFlows wfs;
  generate_workflows(wfs, paras.block_size, tracefile_path, scale_paras, from_file);
  Client client("0.0.0.0", CLIENT_PORT, "0.0.0.0", COORDINATOR_PORT);

  // generate key-value pair
  std::vector<size_t> value_lengths(stripe_num, 0);
  std::vector<std::vector<std::string>> ms_object_keys;
  int obj_id = 0;
  for (int i = 0; i < stripe_num; i++) {
    std::vector<std::string> object_keys;
    for (auto& size : wfs.ms_object_sizes[i]) {
      value_lengths[i] += size;
      object_keys.emplace_back("obj" + std::to_string(obj_id++));
    }
    ms_object_keys.emplace_back(object_keys);
  }
  bool dynamic = true;
  int t = startcase;

  for (; t < 4; ++t) {
    if (t == 1) {
      scale_paras.optimized_recal = false;
    } else if (t == 2) {
      dynamic = false;
      scale_paras.optimized_recal = true;
    } else if (t == 3) {
      paras.ec_type = ECTYPE::UNIFORM_CAUCHY_LRC;
    } 

    print_ec_info(nullptr, paras);

    // set erasure coding parameters
    client.set_ec_parameters(paras);

    // set log level
    client.set_log_level(paras.loglevel);

    struct timeval start_time, end_time;

    #ifdef IN_MEMORY
      std::unordered_map<std::string, std::string> key_value;
      generate_unique_random_strings_difflen(5, stripe_num, value_lengths, key_value);
    #endif

    // set
    double set_time = 0;
    #ifdef IN_MEMORY
      int i = 0;
      for (auto& kv : key_value) {
        gettimeofday(&start_time, NULL);
        double encoding_time = client.set(kv.second, ms_object_keys[i], wfs.ms_object_sizes[i],
            wfs.ms_object_accessrates[0][i], wfs.ms_storge_overhead[i], wfs.ms_g[i]);
        gettimeofday(&end_time, NULL);
        double temp_time = end_time.tv_sec - start_time.tv_sec +
            (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
        set_time += temp_time;
        std::cout << "[SET] set time: " << temp_time << ", encoding time: "
                  << encoding_time << std::endl;
        ++i;
      }
      std::cout << "Total set time: " << set_time << ", average set time:"
                << set_time / stripe_num << std::endl;
    #else
      for (int i = 0; i < stripe_num; i++) {
        std::string readpath = path_prefix + "/../../data/Object";
        double encoding_time = 0;
        gettimeofday(&start_time, NULL);
        if (access(readpath.c_str(), 0) == -1) {
          std::cout << "[Client] file does not exist!" << std::endl;
          exit(-1);
        } else {
          char *buf = new char[value_lengths[i] + 1];
          std::ifstream ifs(readpath);
          ifs.read(buf, value_lengths[i]);
          buf[value_lengths[i]] = '\0';
          encoding_time = client.set(std::string(buf), ms_object_keys[i], 
              wfs.ms_object_sizes[i], wfs.ms_object_accessrates[0][i], 
              wfs.ms_storge_overhead[i], wfs.ms_g[i]);
          ifs.close();
          delete buf;
        }
        gettimeofday(&end_time, NULL);
        double temp_time = end_time.tv_sec - start_time.tv_sec +
            (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
        set_time += temp_time;
        std::cout << "[SET] set time: " << temp_time << ", encoding time: "
                  << encoding_time << std::endl;
      }
      std::cout << "Total set time: " << set_time << ", average set time:"
                << set_time / stripe_num << std::endl;
    #endif

    int test_time = scale_paras.test_time;
    srand(time(NULL));

    for (int tt = 0; tt < test_time + 1; tt++) {
      if (tt <= test_time) {
        // test repair
        if (failed_num == 1) {
          test_single_block_repair_lrc_periodically(client, wfs.ms_object_sizes,
              wfs.ms_object_accessrates[tt], wfs.ms_storge_overhead, wfs.ms_g, paras.block_size);
        } else if (failed_num == 2) {
          test_multi_blocks_repair_lrc_periodically(client, wfs.ms_object_sizes,
              wfs.ms_object_accessrates[tt], wfs.ms_storge_overhead, wfs.ms_g, paras.block_size);
        }
        if (tt == test_time) {
          break;
        }
      }

      client.update_hotness(wfs.ms_object_accessrates[tt + 1]);
      
      // scale
      auto resp = client.scale(scale_paras.storage_overhead_upper_bound,
              scale_paras.gamma, scale_paras.optimized_recal, dynamic);
      if (resp.ifscaled) {
        std::cout << "[SCALE] scale time: " << resp.scaling_time
                  << ", computing time: " << resp.computing_time
                  << ", cross-cluster time: " << resp.cross_cluster_time
                  << ", meta time: " << resp.meta_time
                  << ", cross-cluster-count: " << resp.cross_cluster_transfers
                  << ", I/Os = " << resp.io_cnt
                  << std::endl;
      }
      std::cout << "Last time: storage_overhead = " << resp.old_storage_overhead
                << ", avg_hotness = " << resp.old_hotness
                << "\nNow: storage_overhead = " << resp.new_storage_overhead
                << ", avg_hotness = " << resp.new_hotness << std::endl;
    }

    // delete
    client.delete_all_stripes();
  }
}

void test_repair_performance_periodically(
    std::string path_prefix, std::string tracefile,
    int stripe_num, const ParametersInfo& paras,
    const ScaleParameters& scale_paras, int failed_num)
{
  std::string tracefile_path = path_prefix + "/../../tracefile/" + tracefile;
  std::vector<std::vector<size_t>> ms_object_sizes;
  std::vector<std::vector<unsigned int>> ms_object_accessrates;
  std::vector<float> ms_storge_overhead;
  std::vector<int> ms_g;
  parse_tracefile(tracefile_path, paras.block_size, ms_storge_overhead, ms_g,
      ms_object_sizes, ms_object_accessrates);

  Client client("0.0.0.0", CLIENT_PORT, "0.0.0.0", COORDINATOR_PORT);

  // set erasure coding parameters
  client.set_ec_parameters(paras);

  // set log level
  client.set_log_level(paras.loglevel);

  struct timeval start_time, end_time;
  // generate key-value pair
  std::vector<size_t> value_lengths(stripe_num, 0);
  std::vector<std::vector<std::string>> ms_object_keys;
  int obj_id = 0;
  for (int i = 0; i < stripe_num; i++) {
    std::vector<std::string> object_keys;
    for (auto& size : ms_object_sizes[i]) {
      value_lengths[i] += size;
      object_keys.emplace_back("obj" + std::to_string(obj_id++));
    }
    ms_object_keys.emplace_back(object_keys);
  }
  #ifdef IN_MEMORY
    std::unordered_map<std::string, std::string> key_value;
    generate_unique_random_strings_difflen(5, stripe_num, value_lengths, key_value);
  #endif

  // set
  double set_time = 0;
  #ifdef IN_MEMORY
    int i = 0;
    for (auto& kv : key_value) {
      gettimeofday(&start_time, NULL);
      double encoding_time = client.set(kv.second, ms_object_keys[i], ms_object_sizes[i],
          ms_object_accessrates[i], ms_storge_overhead[i], ms_g[i]);
      gettimeofday(&end_time, NULL);
      double temp_time = end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
      set_time += temp_time;
      std::cout << "[SET] set time: " << temp_time << ", encoding time: "
                << encoding_time << std::endl;
      ++i;
    }
    std::cout << "Total set time: " << set_time << ", average set time:"
              << set_time / stripe_num << std::endl;
  #else
    for (int i = 0; i < stripe_num; i++) {
      std::string readpath = path_prefix + "/../../data/Object";
      double encoding_time = 0;
      gettimeofday(&start_time, NULL);
      if (access(readpath.c_str(), 0) == -1) {
        std::cout << "[Client] file does not exist!" << std::endl;
        exit(-1);
      } else {
        char *buf = new char[value_lengths[i]];
        std::ifstream ifs(readpath);
        ifs.read(buf, value_lengths[i]);
        encoding_time = client.set(std::string(buf), ms_object_keys[i], 
            ms_object_sizes[i], ms_object_accessrates[i], 
            ms_storge_overhead[i], ms_g[i]);
        ifs.close();
        delete buf;
      }
      gettimeofday(&end_time, NULL);
      double temp_time = end_time.tv_sec - start_time.tv_sec +
          (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
      set_time += temp_time;
      std::cout << "[SET] set time: " << temp_time << ", encoding time: "
                << encoding_time << std::endl;
    }
    std::cout << "Total set time: " << set_time << ", average set time:"
              << set_time / stripe_num << std::endl;
  #endif

  int test_time = scale_paras.test_time;
  srand(time(NULL));

  while (test_time--) {
    // test repair
    if (failed_num == 1) {
      test_single_block_repair_lrc_periodically(client, ms_object_sizes,
          ms_object_accessrates, ms_storge_overhead, ms_g, paras.block_size);
    } else if (failed_num == 2) {
      test_multi_blocks_repair_lrc_periodically(client, ms_object_sizes,
          ms_object_accessrates, ms_storge_overhead, ms_g, paras.block_size);
    }

    double change_rate = scale_paras.change_rate;
    for (auto& object_accessrates : ms_object_accessrates) {
      for (auto& accessrate : object_accessrates) {
        if (rand() / double(RAND_MAX) < change_rate) {
          double randomFactor = 1.0 + ((rand() % 201 - 100) / 100.0);
          accessrate = static_cast<unsigned int>(accessrate * randomFactor);
          if (accessrate == 0) accessrate = 1;
        }
      }
    }

    // get
    double get_time = 0;
    for (int i = 0; i < stripe_num; i++) {
      int obj_idx = 0;
      for (auto& accessrate : ms_object_accessrates[i]) {
        for (int j = 0; j < accessrate; j++) {
          gettimeofday(&start_time, NULL);
          client.get(ms_object_keys[i][obj_idx]);
          gettimeofday(&end_time, NULL);
          double temp_time = end_time.tv_sec - start_time.tv_sec +
              (end_time.tv_usec - start_time.tv_usec) / 1000000;
          get_time += temp_time;
        }
        obj_idx++;
      } 
    }
    std::cout << "Total get time: " << get_time << ", average get time:"
              << get_time / stripe_num << std::endl;
    
    // scale
    auto resp = client.scale(scale_paras.storage_overhead_upper_bound,
            scale_paras.gamma, scale_paras.optimized_recal);
    if (resp.ifscaled) {
      std::cout << "[SCALE] scale time: " << resp.scaling_time
                << ", computing time: " << resp.computing_time
                << ", cross-cluster time: " << resp.cross_cluster_time
                << ", meta time: " << resp.meta_time
                << ", cross-cluster-count: " << resp.cross_cluster_transfers
                << ", I/Os = " << resp.io_cnt
                << std::endl;
    }
    std::cout << "Last time: storage_overhead = " << resp.old_storage_overhead
              << ", avg_hotness = " << resp.old_hotness
              << "\nNow: storage_overhead = " << resp.new_storage_overhead
              << ", avg_hotness = " << resp.new_hotness << std::endl;
  }

  // delete
  client.delete_all_stripes();
}

void test_multi_blocks_repair_performance(
    std::string path_prefix, std::string tracefile,
    int stripe_num, ParametersInfo& paras,
    int failed_num, int startcase = 0)
{
  std::vector<std::vector<size_t>> ms_object_sizes;
  std::vector<std::vector<unsigned int>> ms_object_accessrates;
  std::vector<float> ms_storge_overhead;
  std::vector<int> ms_g;
  std::string tracefile_path = path_prefix + "/../../tracefile/" + tracefile;
  parse_tracefile(tracefile_path, paras.block_size, ms_storge_overhead, ms_g, ms_object_sizes, ms_object_accessrates);

  std::string failures_file_path = path_prefix + "/../../testcase/";
  // generate_random_multi_block_failures_lrc(failures_file_path, stripe_num, paras, failed_num, ms_object_sizes, ms_object_accessrates);
  // return;

  Client client("0.0.0.0", CLIENT_PORT, "0.0.0.0", COORDINATOR_PORT);

  // generate key-value pair
  std::vector<size_t> value_lengths(stripe_num, 0);
  std::vector<std::vector<std::string>> ms_object_keys;
  int obj_id = 0;
  for (int i = 0; i < stripe_num; i++) {
    std::vector<std::string> object_keys;
    for (auto& size : ms_object_sizes[i]) {
      value_lengths[i] += size;
      object_keys.emplace_back("obj" + std::to_string(obj_id++));
    }
    ms_object_keys.emplace_back(object_keys);
  }
  int t = startcase;

  for (; t < 4; ++t) {
    if (t == 1) {
      paras.repair_priority = true;
    } else if (t == 2) {
      paras.repair_priority = true;
      paras.partial_scheme = true;
    } else if (t == 3) {
      paras.repair_priority = true;
      paras.partial_scheme = true;
      paras.partial_decoding = false;
    } 

    print_ec_info(nullptr, paras);

    // set erasure coding parameters
    client.set_ec_parameters(paras);

    // set log level
    client.set_log_level(paras.loglevel);

    struct timeval start_time, end_time;

    #ifdef IN_MEMORY
      std::unordered_map<std::string, std::string> key_value;
      generate_unique_random_strings_difflen(5, stripe_num, value_lengths, key_value);
    #endif

    // set
    double set_time = 0;
    #ifdef IN_MEMORY
      int i = 0;
      for (auto& kv : key_value) {
        gettimeofday(&start_time, NULL);
        double encoding_time = client.set(kv.second, ms_object_keys[i], ms_object_sizes[i],
            ms_object_accessrates[i], ms_storge_overhead[i], ms_g[i]);
        gettimeofday(&end_time, NULL);
        double temp_time = end_time.tv_sec - start_time.tv_sec +
            (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
        set_time += temp_time;
        std::cout << "[SET] set time: " << temp_time << ", encoding time: "
                  << encoding_time << std::endl;
        ++i;
      }
      std::cout << "Total set time: " << set_time << ", average set time:"
                << set_time / stripe_num << std::endl;
    #else
      for (int i = 0; i < stripe_num; i++) {
        std::string readpath = path_prefix + "/../../data/Object";
        double encoding_time = 0;
        gettimeofday(&start_time, NULL);
        if (access(readpath.c_str(), 0) == -1) {
          std::cout << "[Client] file does not exist!" << std::endl;
          exit(-1);
        } else {
          char *buf = new char[value_lengths[i] + 1];
          std::ifstream ifs(readpath);
          ifs.read(buf, value_lengths[i]);
          buf[value_lengths[i]] = '\0';
          encoding_time = client.set(std::string(buf), ms_object_keys[i], 
              ms_object_sizes[i], ms_object_accessrates[i], 
              ms_storge_overhead[i], ms_g[i]);
          ifs.close();
          delete buf;
        }
        gettimeofday(&end_time, NULL);
        double temp_time = end_time.tv_sec - start_time.tv_sec +
            (end_time.tv_usec - start_time.tv_usec) / 1000000.0;
        set_time += temp_time;
        std::cout << "[SET] set time: " << temp_time << ", encoding time: "
                  << encoding_time << std::endl;
      }
      std::cout << "Total set time: " << set_time << ", average set time:"
                << set_time / stripe_num << std::endl;
    #endif

    test_multiple_blocks_repair_lrc_with_testcases(failures_file_path, client, paras, failed_num);

    // delete
    client.delete_all_stripes();
  }
}

int main(int argc, char **argv)
{
  if (argc < 5) {
    std::cout << "./run_client config_file tracefile stripe_num failed_num [startcase] [from_file]" << std::endl;
    exit(0);
  }

  char buff[256];
  getcwd(buff, 256);
  std::string cwf = std::string(argv[0]);
  std::string path_prefix = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1);

  ParametersInfo paras;
  ScaleParameters scale_paras;
  parse_args(nullptr, paras, scale_paras, path_prefix + "/../" + std::string(argv[1]));
  std::string tracefile = std::string(argv[2]);
  int stripe_num = std::stoi(argv[3]);

  int failed_num = std::stoi(argv[4]);
  my_assert(0 <= failed_num && failed_num <= 4);
  int startcase = 0;
  bool from_file = false;
  if (argc == 7) {
    startcase = std::stoi(argv[5]);
    from_file = std::string(argv[6]) == "true";
  }

  test_multi_blocks_repair_performance(path_prefix, tracefile, stripe_num, paras, failed_num, startcase);
  return 0;
  
  ParametersInfo paras2 = paras;
  ScaleParameters scale_paras2 = scale_paras;
  // test_repair_performance_periodically(path_prefix, tracefile, stripe_num, paras, scale_paras, failed_num);
  if (startcase < 4) {
    test_repair_performance_periodically_v2(path_prefix, tracefile, stripe_num, paras, scale_paras, failed_num, from_file, startcase);
  }
  startcase %= 4;
  paras2.placement_rule = PlacementRule::OPTIMAL;
  test_repair_performance_periodically_v2(path_prefix, tracefile, stripe_num, paras2, scale_paras2, failed_num, true, startcase);

  return 0;
}