//
// Created by Yibo Huang on 09/02/24 (Labor Day)!
//

#pragma once

#include <mutex>
#include <list>
#include "stdint.h"

#include "common/CXLMemory.h"
#include "core/Table.h"
#include "protocol/Pasha/MigrationManager.h"

namespace star
{

class PolicyAging : public MigrationManager {
    public:
        struct AgingMeta {
                uint8_t age_counter = 0xFF;
        };

        struct AgingTrackerNode {
                AgingTrackerNode(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row)
                        : row_entity(table, key, row, 0)
                {}

                migrated_row_entity row_entity;
                AgingTrackerNode *next{ nullptr };
                AgingTrackerNode *prev{ nullptr };
        };

        class AgingTracker {
            public:
                AgingTracker()
                : head{nullptr}
                {
                        // this spinlock will be shared between multiple processes
                        pthread_spin_init(&aging_tracker_lock, PTHREAD_PROCESS_SHARED);
                }

                void lock()
                {
                        pthread_spin_lock(&aging_tracker_lock);
                }

                void unlock()
                {
                        pthread_spin_unlock(&aging_tracker_lock);
                }

                // push back to the tail
                void track(AgingTrackerNode *node)
                {
                        if (head == nullptr && tail == nullptr) {
                                head = node;
                                tail = node;
                                node->next = nullptr;
                                node->prev = nullptr;
                        } else {
                                CHECK(head != nullptr);
                                CHECK(tail != nullptr);
                                CHECK(tail->next == nullptr);
                                CHECK(head->prev == nullptr);

                                tail->next = node;
                                node->prev = tail;
                                tail = node;
                        }
                }

                // remove from the list
                void untrack(AgingTrackerNode *node)
                {
                        if (head == nullptr && tail == nullptr) {
                                CHECK(0);
                        } else if (head == tail) {
                                CHECK(node == head);
                                CHECK(node == tail);

                                head = nullptr;
                                tail = nullptr;
                        } else {
                                if (node->prev != nullptr) {
                                        node->prev->next = node->next;
                                }
                                if (node->next != nullptr) {
                                        node->next->prev = node->prev;
                                }

                                if (head == node) {
                                        head = node->next;
                                }
                                if (tail == node) {
                                        tail = node->prev;
                                }
                        }

                        node->prev = nullptr;
                        node->next = nullptr;
                }

                // changes
                // Single pass: right-shift every age counter by 1. Call once per eviction round.
                void age_all_entries()
                {
                        AgingTrackerNode *cur = head;
                        while (cur != nullptr) {
                                AgingMeta *aging_meta = reinterpret_cast<AgingMeta *>(cur->row_entity.migration_manager_meta);
                                aging_meta->age_counter >>= 1;
                                cur = cur->next;
                        }
                }

                // Single pass: return the node with the minimum age_counter (eviction candidate).
                AgingTrackerNode *get_min_age_victim()
                {
                        AgingTrackerNode *victim = nullptr;
                        uint8_t min_age = std::numeric_limits<uint8_t>::max();

                        AgingTrackerNode *cur = head;
                        while (cur != nullptr) {
                                AgingMeta *aging_meta = reinterpret_cast<AgingMeta *>(cur->row_entity.migration_manager_meta);
                                if (aging_meta->age_counter < min_age) {
                                        min_age = aging_meta->age_counter;
                                        victim = cur;
                                }
                                cur = cur->next;
                        }
                        return victim;
                }

                bool is_empty() const { return head == nullptr; }

            private:
                AgingTrackerNode *head{ nullptr };
                AgingTrackerNode *tail{ nullptr };

                pthread_spinlock_t aging_tracker_lock;
        };

        PolicyAging(std::function<migration_result(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &, bool, void *&)> move_from_partition_to_shared_region,
                        std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition,
                        std::function<bool(ITable *, const void *, bool, bool &, void *&)> delete_and_update_next_key_info,
                        uint64_t coordinator_id,
                        uint64_t partition_num,
                        const std::string when_to_move_out_str,
                        uint64_t hw_cc_budget)
        : MigrationManager(move_from_partition_to_shared_region, move_from_shared_region_to_partition, delete_and_update_next_key_info, when_to_move_out_str)
        , hw_cc_budget(hw_cc_budget)
        {
                // changes
                CHECK(MigrationManager::migration_policy_meta_size >= sizeof(AgingMeta));
                aging_trackers = new AgingTracker[partition_num];
                for (int i = 0; i < partition_num; i++) {
                        new(&aging_trackers[i]) AgingTracker();
                }
        }

        void init_migration_policy_metadata(void *migration_policy_meta, ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, uint64_t metadata_size) override
        {
                AgingMeta *aging_meta = reinterpret_cast<AgingMeta *>(migration_policy_meta);
                new(aging_meta) AgingMeta();
        }

        void access_row(void *migration_policy_meta, uint64_t partition_id) override
        {
                AgingMeta *aging_meta = reinterpret_cast<AgingMeta *>(migration_policy_meta);
                aging_meta->age_counter |= 0x80;   // changes
        }

        migration_result move_row_in(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, bool inc_ref_cnt) override
        {
                AgingTracker &aging_tracker = aging_trackers[table->partitionID()];
                void *migration_policy_meta = nullptr;
                migration_result ret = migration_result::FAIL_OOM;

                aging_tracker.lock();
                ret = move_from_partition_to_shared_region(table, key, row, inc_ref_cnt, migration_policy_meta);
                if (ret == migration_result::SUCCESS) {
                        AgingTrackerNode *aging_tracker_node = new AgingTrackerNode(table, key, row);
                        aging_tracker_node->row_entity.migration_manager_meta = migration_policy_meta;
                        aging_tracker.track(aging_tracker_node);
                }
                aging_tracker.unlock();

                return ret;
        }

        bool move_row_out(uint64_t partition_id) override
        {
                AgingTracker &aging_tracker = aging_trackers[partition_id];
                bool ret = false;

                aging_tracker.lock();
                if (cxl_memory.get_stats(CXLMemory::TOTAL_HW_CC_USAGE) < hw_cc_budget) {
                    aging_tracker.unlock();
                    return ret;
                }

                while (true) {
                    if (aging_tracker.is_empty()) break;
                    AgingTrackerNode *victim = aging_tracker.get_min_age_victim();
                    if (victim == nullptr) {
                        break;
                    } else {
                        migrated_row_entity victim_row_entity = victim->row_entity;
                        bool move_out_success = false;
                        move_out_success = move_from_shared_region_to_partition(victim_row_entity.table, victim_row_entity.key, victim_row_entity.local_row);
                        if (move_out_success == true) {
                            aging_tracker.untrack(victim);
                            delete victim;
                            // aging_tracker.reset_cursor();
                            if (cxl_memory.get_stats(CXLMemory::TOTAL_HW_CC_USAGE) < hw_cc_budget) {
                                ret = true;
                                break;
                            }
                        }
                    }
                }
                // aging_tracker.reset_cur_victim();
                aging_tracker.unlock();
                return ret;
        }

        bool delete_specific_row_and_move_out(ITable *table, const void *key, bool is_delete_local) override
        {
            // key is unused
            AgingTracker &aging_tracker = aging_trackers[table->partitionID()];
            void *migration_policy_meta = nullptr;
            bool need_move_out = false, ret = false;

            aging_tracker.lock();

            // delete and update next key information
            ret = delete_and_update_next_key_info(table, key, is_delete_local, need_move_out, migration_policy_meta);
            CHECK(ret == true);
            CHECK(need_move_out == false);

            aging_tracker.unlock();

            return ret;
        }

    private:
        uint64_t hw_cc_budget{ 0 };

        AgingTracker *aging_trackers{ nullptr };
};

} // namespace star
