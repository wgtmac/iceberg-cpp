
#include <chrono>
#include <iostream>
#include <vector>
#include <string>

#define private public
#include "iceberg/snapshot.h"
#undef private
#include "iceberg/manifest/manifest_entry.h"

namespace iceberg {

void RunBenchmark() {
    SnapshotSummaryBuilder builder;
    SnapshotSummaryBuilder::UpdateMetrics metrics;

    // Setup a complex UpdateMetrics manually since fields are public with #define private public
    metrics.added_files_ = 10;
    metrics.removed_files_ = 5;
    metrics.added_records_ = 1000;
    metrics.deleted_records_ = 500;
    metrics.added_size_ = 1234567;
    metrics.removed_size_ = 765432;
    metrics.added_pos_delete_files_ = 2;
    metrics.added_eq_delete_files_ = 1;
    metrics.added_pos_deletes_ = 100;
    metrics.added_eq_deletes_ = 50;
    metrics.trust_size_and_delete_counts_ = true;

    const int iterations = 100000;
    auto start = std::chrono::high_resolution_clock::now();

    volatile size_t total_len = 0;
    for (int i = 0; i < iterations; ++i) {
        std::string summary = builder.PartitionSummary(metrics);
        total_len += summary.length();
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;

    std::cout << "Time for " << iterations << " iterations: " << diff.count() << " seconds" << std::endl;
    std::cout << "Average time: " << (diff.count() / iterations) * 1e6 << " microseconds" << std::endl;
}

}

int main() {
    iceberg::RunBenchmark();
    return 0;
}
