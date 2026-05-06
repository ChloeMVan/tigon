#pragma once

#include <mutex>
#include <list>
#include "stdint.h"

#include "common/CXLMemory.h"
#include "core/Table.h"
#include "protocol/Pasha/MigrationManager.h"

namespace star
{

class PolicyClockAutoScan : public MigrationManager {
    public:
        static constexpr uint32_t SCAN_STREAK_THRESHOLD = 4;

        struct ClockMeta {
                uint8_t second_chance = 0;
                bool is_scan = false;
        };

        struct ClockTrackerNode {
                ClockTrackerNode(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row)
                        : row_entity(table, key, row, 0)
                {}

                migrated_row_entity row_entity;
                ClockTrackerNode *next{ nullptr };
                ClockTrackerNode *prev{ nullptr };
        };

        struct ScanDetector {
                char last_key[migrated_row_entity::max_key_size];
                ITable *last_table{ nullptr };
                uint32_t sequential_count{ 0 };

                bool update(ITable *table, const void *key)
                {
                        if (last_table == table) {
                                if (table->compare_key(last_key, key) < 0) {
                                        sequential_count++;
                                } else {
                                        sequential_count = 0;
                                }
                        } else {
                                sequential_count = 0;
                                last_table = table;
                        }
                        memcpy(last_key, key, table->key_size());
                        return sequential_count >= SCAN_STREAK_THRESHOLD;
                }
        };

        class ClockTracker {
            public:
                ClockTracker()
                : head{nullptr}
                {
                        pthread_spin_init(&clock_tracker_lock, PTHREAD_PROCESS_SHARED);
                }

                void lock()   { pthread_spin_lock(&clock_tracker_lock); }
                void unlock() { pthread_spin_unlock(&clock_tracker_lock); }

                void track(ClockTrackerNode *node)
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

                void untrack(ClockTrackerNode *node)
                {
                        if (head == nullptr && tail == nullptr) {
                                CHECK(0);
                        } else if (head == tail) {
                                CHECK(node == head);
                                CHECK(node == tail);
                                head = nullptr;
                                tail = nullptr;
                        } else {
                                if (node->prev != nullptr) node->prev->next = node->next;
                                if (node->next != nullptr) node->next->prev = node->prev;
                                if (head == node) head = node->next;
                                if (tail == node) tail = node->prev;
                        }
                        node->prev = nullptr;
                        node->next = nullptr;
                }

                ClockTrackerNode *move_forward_and_get_cursor()
                {
                        if (cursor == nullptr) {
                                cursor = head;
                        } else {
                                cursor = cursor->next;
                        }
                        return cursor;
                }

                void reset_cursor() { cursor = nullptr; }

            private:
                ClockTrackerNode *head{ nullptr };
                ClockTrackerNode *tail{ nullptr };
                ClockTrackerNode *cursor{ nullptr };

                pthread_spinlock_t clock_tracker_lock;
        };

        PolicyClockAutoScan(std::function<migration_result(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &, bool, void *&)> move_from_partition_to_shared_region,
                        std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition,
                        std::function<bool(ITable *, const void *, bool, bool &, void *&)> delete_and_update_next_key_info,
                        uint64_t coordinator_id,
                        uint64_t partition_num,
                        const std::string when_to_move_out_str,
                        uint64_t hw_cc_budget)
        : MigrationManager(move_from_partition_to_shared_region, move_from_shared_region_to_partition, delete_and_update_next_key_info, when_to_move_out_str)
        , hw_cc_budget(hw_cc_budget)
        {
                clock_trackers = new ClockTracker[partition_num];
                for (int i = 0; i < partition_num; i++) {
                        new(&clock_trackers[i]) ClockTracker();
                }
        }

        void init_migration_policy_metadata(void *migration_policy_meta, ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, uint64_t metadata_size) override
        {
                ClockMeta *clock_meta = reinterpret_cast<ClockMeta *>(migration_policy_meta);
                new(clock_meta) ClockMeta();
        }

        void access_row(void *migration_policy_meta, uint64_t partition_id) override
        {
                ClockMeta *clock_meta = reinterpret_cast<ClockMeta *>(migration_policy_meta);
                clock_meta->second_chance = 1;
                // access_row is only called on point accesses, so demote any prior scan classification.
                clock_meta->is_scan = false;
        }

        migration_result move_row_in(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row, bool inc_ref_cnt, bool is_scan = false) override
        {
                ClockTracker &clock_tracker = clock_trackers[table->partitionID()];
                void *migration_policy_meta = nullptr;
                migration_result ret = migration_result::FAIL_OOM;

                thread_local ScanDetector scan_detector;

                clock_tracker.lock();

                bool detected_scan = scan_detector.update(table, key);

                ret = move_from_partition_to_shared_region(table, key, row, inc_ref_cnt, migration_policy_meta);
                if (ret == migration_result::SUCCESS) {
                        ClockMeta *clock_meta = reinterpret_cast<ClockMeta *>(migration_policy_meta);
                        clock_meta->is_scan = detected_scan;
                        ClockTrackerNode *clock_tracker_node = new ClockTrackerNode(table, key, row);
                        clock_tracker_node->row_entity.migration_manager_meta = migration_policy_meta;
                        clock_tracker.track(clock_tracker_node);
                }

                clock_tracker.unlock();

                return ret;
        }

        bool move_row_out(uint64_t partition_id) override
        {
                ClockTracker &clock_tracker = clock_trackers[partition_id];
                bool ret = false;

                clock_tracker.lock();
                if (cxl_memory.get_stats(CXLMemory::TOTAL_HW_CC_USAGE) < hw_cc_budget) {
                        clock_tracker.unlock();
                        return ret;
                }

                while (true) {
                        ClockTrackerNode *victim = clock_tracker.move_forward_and_get_cursor();
                        if (victim == nullptr) {
                                break;
                        } else {
                                migrated_row_entity victim_row_entity = victim->row_entity;
                                ClockMeta *clock_meta = reinterpret_cast<ClockMeta *>(victim_row_entity.migration_manager_meta);

                                // Scan items bypass the second-chance protection and are evicted immediately.
                                // Point items get one second chance before eviction.
                                if (!clock_meta->is_scan && clock_meta->second_chance == 1) {
                                        clock_meta->second_chance = 0;
                                        continue;
                                }

                                bool move_out_success = move_from_shared_region_to_partition(victim_row_entity.table, victim_row_entity.key, victim_row_entity.local_row);
                                if (move_out_success == true) {
                                        clock_tracker.move_forward_and_get_cursor();
                                        clock_tracker.untrack(victim);
                                        if (cxl_memory.get_stats(CXLMemory::TOTAL_HW_CC_USAGE) < hw_cc_budget) {
                                                ret = true;
                                                break;
                                        }
                                }
        		}
                }

                clock_tracker.unlock();

                return ret;
        }

        bool delete_specific_row_and_move_out(ITable *table, const void *key, bool is_delete_local) override
        {
                ClockTracker &clock_tracker = clock_trackers[table->partitionID()];
                void *migration_policy_meta = nullptr;
                bool need_move_out = false, ret = false;

                clock_tracker.lock();
                ret = delete_and_update_next_key_info(table, key, is_delete_local, need_move_out, migration_policy_meta);
                CHECK(ret == true);
                CHECK(need_move_out == false);
                clock_tracker.unlock();

                return ret;
        }

    private:
        uint64_t hw_cc_budget{ 0 };

        ClockTracker *clock_trackers{ nullptr };
};

} // namespace star
