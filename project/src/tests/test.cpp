#include "lrc.h"

using namespace ECProject;

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

void generate_recalculation_plans(
          std::vector<std::vector<int>>& plans,
          Non_Uni_LRC* old_lrc, Non_Uni_LRC* new_lrc)
  {
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
                ++gid;
                break;
              }
            }
            if (!flag) {
              for (auto num : new_groups[gid]) {
                if (num != k + g + gid) {
                  plans[gid].emplace_back(num);
                }
              }
              ++gid;
            }
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

int main()
{
  Non_Uni_LRC* old_ec = new Non_Uni_LRC(16, 3, 2);
  Non_Uni_LRC* new_ec = new Non_Uni_LRC(16, 3, 2);
  old_ec->krs.emplace_back(std::make_pair(4, 0));
  old_ec->krs.emplace_back(std::make_pair(3, 0));
  old_ec->krs.emplace_back(std::make_pair(1, 0));
  old_ec->krs.emplace_back(std::make_pair(2, 0));
  old_ec->krs.emplace_back(std::make_pair(6, 5));
  old_ec->krs.emplace_back(std::make_pair(13, 7));
  new_ec->krs.emplace_back(std::make_pair(4, 0));
  new_ec->krs.emplace_back(std::make_pair(3, 0));
  new_ec->krs.emplace_back(std::make_pair(1, 0));
  new_ec->krs.emplace_back(std::make_pair(2, 0));
  new_ec->krs.emplace_back(std::make_pair(6, 6));
  new_ec->krs.emplace_back(std::make_pair(12, 6));
  std::vector<std::vector<int>> plans;
  generate_recalculation_plans(plans, old_ec, new_ec);
  for (auto plan : plans) {
    for (auto num : plan) {
      std::cout << num << " ";
    }
    std::cout << std::endl;
  }
  delete old_ec;
  delete new_ec;
  return 0;
}