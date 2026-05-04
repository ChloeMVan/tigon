
#pragma once

#include "stdint.h"

#include "common/CXLMemory.h"
#include "core/Table.h"
#include "protocol/Pasha/MigrationManager.h"

namespace star
{

class PolicyAdaptiveAging : public MigrationManager {
public:
    struct AAMeta {
        uint8_t counter = 0x80;
        bool is_scan = false;
    };

    struct AATrackerNode {
        AATrackerNode(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row)
            : row_entity(table, key, row, 0)
        {}

        migrated_row_entity row_entity;
        AATrackerNode *next{ nullptr };
        AATrackerNode *prev{ nullptr };
    };

    class AATracker {
    public:
        AATracker()
        {
            pthread_spin_init(&tracker_lock, PTHREAD_PROCESS_SHARED);
        }

        void lock() { pthread_spin_lock(&tracker_lock); }
        void unlock() { pthread_spin_unlock(&tracker_lock); }

        void track(AATrackerNode *node)
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

        void untrack(AATrackerNode *node)
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

        AATrackerNode *move_forward_and_get_cursor()
        {
            cursor = (cursor == nullptr) ? head : cursor->next;
            return cursor;
        }

        void reset_cursor() { cursor = nullptr; }

    private:
        AATrackerNode *head{ nullptr };
        AATrackerNode *tail{ nullptr };
        AATrackerNode *cursor{ nullptr };
        pthread_spinlock_t tracker_lock;
    };

    PolicyAdaptiveAging(
        std::function<migration_result(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &, bool, void *&)> move_from_partition_to_shared_region,
        std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition,
        std::function<bool(ITable *, const void *, bool, bool &, void *&)> delete_and_update_next_key_info,
        uint64_t coordinator_id,
        uint64_t partition_num,
        const std::string when_to_move_out_str,
        uint64_t hw_cc_budget,
        uint8_t scan_refresh_mask = 0x40)
    : MigrationManager(move_from_partition_to_shared_region, move_from_shared_region_to_partition, delete_and_update_next_key_info, when_to_move_out_str)
    , hw_cc_budget(hw_cc_budget)
    , scan_refresh_mask(scan_refresh_mask)
    {
        trackers = new AATracker[partition_num];
        for (int i = 0; i < partition_num; i++) {
            new(&trackers[i]) AATracker();
        }
    }

    void init_migration_policy_metadata(void *migration_policy_meta, ITable *table, const void *key,
                                         const std::tuple<MetaDataType *, void *> &row, uint64_t metadata_size) override
    {
        AAMeta *meta = reinterpret_cast<AAMeta *>(migration_policy_meta);
        new(meta) AAMeta();
    }

    void access_row(void *migration_policy_meta, uint64_t partition_id) override
    {
        AAMeta *meta = reinterpret_cast<AAMeta *>(migration_policy_meta);
        if (!meta->is_scan) {
            meta->counter |= 0x80;
        } else {
            meta->counter |= scan_refresh_mask;
        }
    }

    migration_result move_row_in(ITable *table, const void *key,
                                  const std::tuple<MetaDataType *, void *> &row,
                                  bool inc_ref_cnt, bool is_scan = false) override
    {
        AATracker &tracker = trackers[table->partitionID()];
        void *migration_policy_meta = nullptr;
        migration_result ret = migration_result::FAIL_OOM;

        tracker.lock();
        ret = move_from_partition_to_shared_region(table, key, row, inc_ref_cnt, migration_policy_meta);
        if (ret == migration_result::SUCCESS) {
            AAMeta *meta = reinterpret_cast<AAMeta *>(migration_policy_meta);
            meta->is_scan = is_scan;
            if (is_scan) {
                meta->counter = 0x40;
            }
            AATrackerNode *node = new AATrackerNode(table, key, row);
            node->row_entity.migration_manager_meta = migration_policy_meta;
            tracker.track(node);
        }
        tracker.unlock();

        return ret;
    }

    bool move_row_out(uint64_t partition_id) override
    {
        AATracker &tracker = trackers[partition_id];
        bool ret = false;

        tracker.lock();
        if (cxl_memory.get_stats(CXLMemory::TOTAL_HW_CC_USAGE) < hw_cc_budget) {
            tracker.unlock();
            return ret;
        }

        while (true) {
            AATrackerNode *min_victim = nullptr;
            uint8_t min_counter = UINT8_MAX;
            bool min_is_scan = false;

            while (true) {
                AATrackerNode *node = tracker.move_forward_and_get_cursor();
                if (node == nullptr) break;

                AAMeta *meta = reinterpret_cast<AAMeta *>(node->row_entity.migration_manager_meta);
                meta->counter >>= 1;

                bool better = (meta->counter < min_counter) ||
                              (meta->counter == min_counter && meta->is_scan && !min_is_scan);
                if (better) {
                    min_counter = meta->counter;
                    min_victim = node;
                    min_is_scan = meta->is_scan;
                }
                if (min_counter == 0 && min_is_scan) break;
            }

            if (min_victim == nullptr) break;

            migrated_row_entity victim = min_victim->row_entity;
            if (move_from_shared_region_to_partition(victim.table, victim.key, victim.local_row)) {
                tracker.untrack(min_victim);
                delete min_victim;
                tracker.reset_cursor();
                if (cxl_memory.get_stats(CXLMemory::TOTAL_HW_CC_USAGE) < hw_cc_budget) {
                    ret = true;
                    break;
                }
            } else {
                break;
            }
        }

        tracker.unlock();
        return ret;
    }

    bool delete_specific_row_and_move_out(ITable *table, const void *key, bool is_delete_local) override
    {
        AATracker &tracker = trackers[table->partitionID()];
        void *migration_policy_meta = nullptr;
        bool need_move_out = false, ret = false;

        tracker.lock();
        ret = delete_and_update_next_key_info(table, key, is_delete_local, need_move_out, migration_policy_meta);
        CHECK(ret == true);
        CHECK(need_move_out == false);
        tracker.unlock();

        return ret;
    }

private:
    uint64_t hw_cc_budget{ 0 };
    uint8_t scan_refresh_mask{ 0x40 };
    AATracker *trackers{ nullptr };
};

} // namespace star
