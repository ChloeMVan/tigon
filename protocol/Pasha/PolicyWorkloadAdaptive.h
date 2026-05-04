
#pragma once

#include "stdint.h"

#include "common/CXLMemory.h"
#include "core/Table.h"
#include "protocol/Pasha/MigrationManager.h"

namespace star
{

class PolicyWorkloadAdaptive : public MigrationManager {
public:
    struct WAMeta {
        uint8_t counter = 0x80;
        bool is_scan = false;
    };

    struct WATrackerNode {
        WATrackerNode(ITable *table, const void *key, const std::tuple<MetaDataType *, void *> &row)
            : row_entity(table, key, row, 0)
        {}

        migrated_row_entity row_entity;
        WATrackerNode *next{ nullptr };
        WATrackerNode *prev{ nullptr };
    };

    class WATracker {
    public:
        WATracker()
        {
            pthread_spin_init(&tracker_lock, PTHREAD_PROCESS_SHARED);
        }

        void lock() { pthread_spin_lock(&tracker_lock); }
        void unlock() { pthread_spin_unlock(&tracker_lock); }

        void track(WATrackerNode *node)
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

        void untrack(WATrackerNode *node)
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

        WATrackerNode *move_forward_and_get_cursor()
        {
            cursor = (cursor == nullptr) ? head : cursor->next;
            return cursor;
        }

        void reset_cursor() { cursor = nullptr; }

    private:
        WATrackerNode *head{ nullptr };
        WATrackerNode *tail{ nullptr };
        WATrackerNode *cursor{ nullptr };
        pthread_spinlock_t tracker_lock;
    };

    PolicyWorkloadAdaptive(
        std::function<migration_result(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &, bool, void *&)> move_from_partition_to_shared_region,
        std::function<bool(ITable *, const void *, const std::tuple<std::atomic<uint64_t> *, void *> &)> move_from_shared_region_to_partition,
        std::function<bool(ITable *, const void *, bool, bool &, void *&)> delete_and_update_next_key_info,
        uint64_t coordinator_id,
        uint64_t partition_num,
        const std::string when_to_move_out_str,
        uint64_t hw_cc_budget)
    : MigrationManager(move_from_partition_to_shared_region, move_from_shared_region_to_partition, delete_and_update_next_key_info, when_to_move_out_str)
    , hw_cc_budget(hw_cc_budget)
    {
        trackers = new WATracker[partition_num];
        for (int i = 0; i < partition_num; i++) {
            new(&trackers[i]) WATracker();
        }
    }

    void init_migration_policy_metadata(void *migration_policy_meta, ITable *table, const void *key,
                                         const std::tuple<MetaDataType *, void *> &row, uint64_t metadata_size) override
    {
        WAMeta *meta = reinterpret_cast<WAMeta *>(migration_policy_meta);
        new(meta) WAMeta();
    }

    void access_row(void *migration_policy_meta, uint64_t partition_id) override
    {
        WAMeta *meta = reinterpret_cast<WAMeta *>(migration_policy_meta);
        // Only restore the high bit for point-access rows; scan rows age out without reprieve
        if (!meta->is_scan) {
            meta->counter |= 0x80;
        }
    }

    migration_result move_row_in(ITable *table, const void *key,
                                  const std::tuple<MetaDataType *, void *> &row,
                                  bool inc_ref_cnt, bool is_scan = false) override
    {
        WATracker &tracker = trackers[table->partitionID()];
        void *migration_policy_meta = nullptr;
        migration_result ret = migration_result::FAIL_OOM;

        tracker.lock();
        ret = move_from_partition_to_shared_region(table, key, row, inc_ref_cnt, migration_policy_meta);
        if (ret == migration_result::SUCCESS) {
            WAMeta *meta = reinterpret_cast<WAMeta *>(migration_policy_meta);
            meta->is_scan = is_scan;
            // Scan rows enter with a halved counter so they become eviction candidates sooner
            if (is_scan) {
                meta->counter = 0x40;
            }
            WATrackerNode *node = new WATrackerNode(table, key, row);
            node->row_entity.migration_manager_meta = migration_policy_meta;
            tracker.track(node);
        }
        tracker.unlock();

        return ret;
    }

    bool move_row_out(uint64_t partition_id) override
    {
        WATracker &tracker = trackers[partition_id];
        bool ret = false;

        tracker.lock();
        if (cxl_memory.get_stats(CXLMemory::TOTAL_HW_CC_USAGE) < hw_cc_budget) {
            tracker.unlock();
            return ret;
        }

        while (true) {
            WATrackerNode *min_victim = nullptr;
            uint8_t min_counter = UINT8_MAX;
            bool min_is_scan = false;

            while (true) {
                WATrackerNode *node = tracker.move_forward_and_get_cursor();
                if (node == nullptr) break;

                WAMeta *meta = reinterpret_cast<WAMeta *>(node->row_entity.migration_manager_meta);
                meta->counter >>= 1;

                // Prefer scan rows as victims when their counters are equal
                bool better = (meta->counter < min_counter) ||
                              (meta->counter == min_counter && meta->is_scan && !min_is_scan);
                if (better) {
                    min_counter = meta->counter;
                    min_victim = node;
                    min_is_scan = meta->is_scan;
                }
                // A scan row at zero is the worst possible candidate; evict immediately
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
        WATracker &tracker = trackers[table->partitionID()];
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
    WATracker *trackers{ nullptr };
};

} 
