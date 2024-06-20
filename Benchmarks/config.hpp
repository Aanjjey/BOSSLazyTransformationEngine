#ifndef BOSSBENCHMARKS_CONFIG_HPP
#define BOSSBENCHMARKS_CONFIG_HPP

#include "ITTNotifySupport.hpp"
#include <string>
#include <vector>

extern std::string tpch_filePath_prefix;

extern VTuneAPIInterface vtune;

extern bool USING_COORDINATOR_ENGINE;
extern bool VERBOSE_QUERY_OUTPUT;
extern bool VERY_VERBOSE_QUERY_OUTPUT;
extern bool VERIFY_QUERY_OUTPUT;
extern bool ENABLE_CONSTRAINTS;
extern int BENCHMARK_MIN_WARMPUP_ITERATIONS;
extern int BENCHMARK_MIN_WARMPUP_TIME;
extern uint64_t SPAN_SIZE_FOR_NON_ARROW;
extern int VELOX_INTERNAL_BATCH_SIZE;
extern int VELOX_MINIMUM_OUTPUT_BATCH_SIZE;

extern std::vector<std::string> librariesToTest;
extern std::string storageLibrary;
extern int latestDataSize; // Scale factor for TPCH and num of elements for custom
extern int latestBlockSize;
extern std::string latestDataSet;

extern int googleBenchmarkApplyParameterHelper;

enum DATASETS { TPCH = 0, GROUP = 100 };

#endif // BOSSBENCHMARKS_CONFIG_HPP
