#include <BOSS.hpp>
#include <ExpressionUtilities.hpp>

#include "config.hpp"
#include "dataGeneration.cpp"
#include "group.cpp"
#include "partition.cpp"
#include "select.cpp"
#include "tpch.cpp"
#include "utilities.cpp"
#include <benchmark/benchmark.h>
#include <iostream>
#include <set>
#include <string>
#include <vector>

using namespace boss::utilities;

VTuneAPIInterface vtune{"BOSS"};

bool USING_COORDINATOR_ENGINE = false;
bool VERBOSE_QUERY_OUTPUT = false;
bool VERY_VERBOSE_QUERY_OUTPUT = false;
bool VERIFY_QUERY_OUTPUT = false;
bool ENABLE_CONSTRAINTS = false;
int BENCHMARK_MIN_WARMPUP_ITERATIONS = 3;
int BENCHMARK_MIN_WARMPUP_TIME = 1;
uint64_t SPAN_SIZE_FOR_NON_ARROW = 1U << 23U;
bool BENCHMARK_STORAGE_BLOCK_SIZE = false;
int64_t DEFAULT_STORAGE_BLOCK_SIZE = 0; // 0: keep storage's default
int VELOX_INTERNAL_BATCH_SIZE = 0;
int VELOX_MINIMUM_OUTPUT_BATCH_SIZE = 0;

std::vector<std::string> librariesToTest = {};
std::string storageLibrary = {};
int latestDataSize = -1;
int latestBlockSize = -1;
std::string latestDataSet;

int googleBenchmarkApplyParameterHelper = -1;

static void releaseBOSSEngines() {
  // make sure to release engines in reverse order of evaluation
  // (important for data ownership across engines)
  auto reversedLibraries = librariesToTest;
  std::reverse(reversedLibraries.begin(), reversedLibraries.end());
  boss::expressions::ExpressionSpanArguments spans;
  spans.emplace_back(boss::expressions::Span<std::string>(reversedLibraries));
  boss::evaluate("ReleaseEngines"_(boss::ComplexExpression("List"_, {}, {}, std::move(spans))));
}

void initAndRunBenchmarks(int argc, char** argv) {
  std::set<int> tpchQueriesToBenchmark;

  for(int i = 0; i < argc; ++i) {
    if(std::string("--library") == argv[i]) {
      if(++i < argc) {
        librariesToTest.emplace_back(argv[i]);
      }
    } else if(std::string("--benchmark-min-warmup-iterations") == argv[i]) {
      if(++i < argc) {
        BENCHMARK_MIN_WARMPUP_ITERATIONS = atoi(argv[i]);
      }
    } else if(std::string("--benchmark-min-warmup-time") == argv[i]) {
      if(++i < argc) {
        BENCHMARK_MIN_WARMPUP_TIME = atoi(argv[i]);
      }
    } else if(std::string("--verbose-query-output") == argv[i] || std::string("-v") == argv[i]) {
      VERBOSE_QUERY_OUTPUT = true;
    } else if(std::string("--very-verbose-query-output") == argv[i] ||
              std::string("-vv") == argv[i]) {
      VERBOSE_QUERY_OUTPUT = true;
      VERY_VERBOSE_QUERY_OUTPUT = true;
    } else if(std::string("--verify-query-output") == argv[i]) {
      VERIFY_QUERY_OUTPUT = true;
    } else if(std::string("--enable-constraints") == argv[i]) {
      ENABLE_CONSTRAINTS = true;
    } else if(std::string("--using-coordinator-engine") == argv[i] ||
              std::string("-ce") == argv[i]) {
      USING_COORDINATOR_ENGINE = true;
    } else if(std::string("--span-size-for-non-arrow") == argv[i] ||
              std::string("-ss") == argv[i]) {
      if(++i < argc) {
        SPAN_SIZE_FOR_NON_ARROW = atoi(argv[i]);
      }
    } else if(std::string("--benchmark-storage-block-size") == argv[i]) {
      BENCHMARK_STORAGE_BLOCK_SIZE = true;
    } else if(std::string("--default-storage-block-size") == argv[i]) {
      if(++i < argc) {
        DEFAULT_STORAGE_BLOCK_SIZE = atoi(argv[i]);
      }
    } else if(std::string("--velox-internal-batch-size") == argv[i]) {
      if(++i < argc) {
        VELOX_INTERNAL_BATCH_SIZE = atoi(argv[i]);
      }
    } else if(std::string("--velox-minimum-output-batch-size") == argv[i]) {
      if(++i < argc) {
        VELOX_MINIMUM_OUTPUT_BATCH_SIZE = atoi(argv[i]);
      }
    } else if(std::string("--etl") == argv[i]) {
      tpchQueriesToBenchmark.insert({LINE_VIEW_03, LINE_VIEW_04, BUTTERFLY_1_VIEW_12, BUTTERFLY_1_VIEW_13});
    } else if(std::string("--etl-line") == argv[i]) {
      tpchQueriesToBenchmark.insert({LINE_VIEW_03, LINE_VIEW_04});
    } else if(std::string("--etl-butterfly") == argv[i]) {;
      tpchQueriesToBenchmark.insert({BUTTERFLY_1_VIEW_12, BUTTERFLY_1_VIEW_13});
    } else if(std::string("--etl-transformation") == argv[i]) {;
      tpchQueriesToBenchmark.insert({LINE, BUTTERFLY});
    } else if(std::string("--tpch") == argv[i]) {
      tpchQueriesToBenchmark.insert({TPCH_Q1, TPCH_Q3, TPCH_Q6, TPCH_Q9, TPCH_Q18});
    } else if(std::string("--tpch-q1") == argv[i]) {
      tpchQueriesToBenchmark.insert(TPCH_Q1);
    } else if(std::string("--tpch-q3") == argv[i]) {
      tpchQueriesToBenchmark.insert(TPCH_Q3);
    } else if(std::string("--tpch-q3-first-join-only") == argv[i]) {
      tpchQueriesToBenchmark.insert(TPCH_Q3_FIRST_JOIN_ONLY);
    } else if(std::string("--tpch-q3-no-top") == argv[i]) {
      tpchQueriesToBenchmark.insert(TPCH_Q3_NOTOP);
    } else if(std::string("--tpch-q6") == argv[i]) {
      tpchQueriesToBenchmark.insert(TPCH_Q6);
    } else if(std::string("--tpch-q9") == argv[i]) {
      tpchQueriesToBenchmark.insert(TPCH_Q9);
    } else if(std::string("--tpch-q9-split-two-keys") == argv[i]) {
      tpchQueriesToBenchmark.insert(TPCH_Q9_SPLIT_TWO_KEYS);
    } else if(std::string("--tpch-q18") == argv[i]) {
      tpchQueriesToBenchmark.insert(TPCH_Q18);
    } else if(std::string("--tpch-q18-no-top") == argv[i]) {
      tpchQueriesToBenchmark.insert(TPCH_Q18_NOTOP);
    } else if(std::string("--tpch-q18-subquery") == argv[i]) {
      tpchQueriesToBenchmark.insert(TPCH_Q18_SUBQUERY);
    } else if(std::string("--tpch-clustered") == argv[i]) {
      /* register TPC-H Q6 clustering benchmarks */
      int dataSize = 1000;
      googleBenchmarkApplyParameterHelper = dataSize;
      std::ostringstream testName;
      auto const& queryName =
          tpchQueryNames()[static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q6)];
      testName << queryName << "/";
      testName << dataSize << "MB";
      benchmark::RegisterBenchmark(testName.str(), tpch_q6_clustering_sweep_Benchmark, dataSize)
          ->MeasureProcessCPUTime()
          ->UseRealTime()
          ->Apply([](benchmark::internal::Benchmark* b) {
            std::string filepath = tpch_filePath_prefix + "data/tpch_" +
                                   std::to_string(googleBenchmarkApplyParameterHelper) +
                                   "MB/lineitem.tbl";
            std::vector<int> thresholds = generateLogDistribution<int>(
                30, 1, static_cast<double>(getNumberOfRowsInTable(filepath)));
            for(int value : thresholds) {
              b->Arg(value);
            }
          });
    } else if(std::string("--select-selectivity") == argv[i]) {
      /* register selectivity sweep benchmarks */
      int dataSize = 25 * 1000 * 1000;
      std::ostringstream testName;
      std::string queryName = "selectivity_sweep_uniform_dis";
      testName << queryName << ",";
      testName << dataSize << " tuples";
      benchmark::RegisterBenchmark(testName.str(), selectivity_sweep_uniform_dis_Benchmark,
                                   dataSize, queryName)
          ->MeasureProcessCPUTime()
          ->UseRealTime()
          ->Apply([](benchmark::internal::Benchmark* b) {
            std::vector<int> thresholds =
                generateLogDistribution<int>(30, 1, 10001); // SELECT less than (0-100%)
            for(int value : thresholds) {
              b->Arg(value);
            }
          });
    } else if(std::string("--select-randomness") == argv[i]) {
      /* register randomness sweep benchmarks */
      int dataSize = 25 * 1000 * 1000;
      std::ostringstream testName;
      std::string queryName = "randomness_sweep_sorted_dis";
      testName << queryName << "/";
      testName << dataSize << " tuples";
      benchmark::RegisterBenchmark(testName.str(), randomness_sweep_sorted_dis_Benchmark, dataSize,
                                   queryName)
          ->MeasureProcessCPUTime()
          ->UseRealTime()
          ->Apply([](benchmark::internal::Benchmark* b) {
            std::vector<float> thresholds = generateLogDistribution<float>(15, 0.1, 100);
            for(float value : thresholds) {
              b->Arg(static_cast<int>(value * 100)); // Pass value to benchmark as an integer
            }
          });
    } else if(std::string("--select-varyLength") == argv[i]) {
      /* register multi-section sweep benchmark with varying section length */
      int dataSize = 100 * 1000 * 1000;
      std::ostringstream testName;
      std::string queryName = "section_length_sweep_step_selectivity_change";
      testName << queryName << "/";
      testName << dataSize << " tuples";
      benchmark::RegisterBenchmark(testName.str(),
                                   section_length_sweep_step_selectivity_change_Benchmark, dataSize,
                                   queryName)
          ->MeasureProcessCPUTime()
          ->UseRealTime()
          ->Apply([](benchmark::internal::Benchmark* b) {
            std::vector<int> thresholds = generateLogDistribution<int>(30, 10'000, 10'000'000);
            for(int value : thresholds) {
              b->Arg(value);
            }
          });
    } else if(std::string("--select-varyRatio") == argv[i]) {
      /* register multi-section sweep benchmark with varying section ratio */
      int dataSize = 100 * 1000 * 1000;
      std::ostringstream testName;
      std::string queryName = "section_ratio_sweep_step_selectivity_change";
      testName << queryName << "/";
      testName << dataSize << " tuples";
      benchmark::RegisterBenchmark(testName.str(),
                                   section_ratio_sweep_step_selectivity_change_Benchmark, dataSize,
                                   queryName)
          ->MeasureProcessCPUTime()
          ->UseRealTime()
          ->Apply([](benchmark::internal::Benchmark* b) {
            std::vector<float> thresholds = generateLinearDistribution<float>(21, 0, 1);
            for(float value : thresholds) {
              b->Arg(static_cast<int>(value * 100)); // Pass value to benchmark as an integer
            }
          });
    } else if(std::string("--partition-clustering") == argv[i]) {
      /* register partition clustering sweep benchmarks */
      int dataSize = 100 * 1000 * 1000; // Change ~12 lines below too - must match
      int upperBound = dataSize;
      int cardinality = dataSize;
      std::ostringstream testName;
      std::string queryName = "partition_clustering_sweep";
      testName << queryName << "/";
      testName << dataSize << " tuples";
      benchmark::RegisterBenchmark(testName.str(), partition_clustering_sweep_Benchmark, dataSize,
                                   upperBound, cardinality, queryName)
          ->MeasureProcessCPUTime()
          ->UseRealTime()
          ->Apply([](benchmark::internal::Benchmark* b) {
            std::vector<int> thresholds = generateLogDistribution<int>(15, 1, 100 * 1000 * 1000);
            for(auto& value : thresholds) {
              b->Arg(value);
            }
          });
    } else if(std::string("--partition-cardinality") == argv[i]) {
      /* register partition cardinality sweep benchmarks */
      int dataSize = 100 * 1000 * 1000;  // Change ~12 lines below too - must match
      int upperBound = 20 * 1000 * 1000; // TBC
      std::ostringstream testName;
      std::string queryName = "partition_cardinality_sweep";
      testName << queryName << "/";
      testName << dataSize << " tuples";
      benchmark::RegisterBenchmark(testName.str(), partition_cardinality_sweep_Benchmark, dataSize,
                                   upperBound, queryName)
          ->MeasureProcessCPUTime()
          ->UseRealTime()
          ->Apply([](benchmark::internal::Benchmark* b) {
            std::vector<int> thresholds = generateLogDistribution<int>(15, 1, 100 * 1000 * 1000);
            for(auto& value : thresholds) {
              b->Arg(value);
            }
          });
    } else if(std::string("--group-cardinality") == argv[i]) {
      /* register group cardinality sweep benchmarks */
      int dataSize = 20 * 1000 * 1000; // Change ~12 lines below too - must match
      int upperBound = 20 * 1000 * 1000;
      std::ostringstream testName;
      std::string queryName = "group_cardinality_sweep";
      testName << queryName << "/";
      testName << dataSize << " tuples";
      benchmark::RegisterBenchmark(testName.str(), group_cardinality_sweep_Benchmark, dataSize,
                                   upperBound, queryName)
          ->MeasureProcessCPUTime()
          ->UseRealTime()
          ->Apply([](benchmark::internal::Benchmark* b) {
            std::vector<int> thresholds = generateLogDistribution<int>(15, 1, 20 * 1000 * 1000);
            for(auto& value : thresholds) {
              b->Arg(value);
            }
          });
    } else if(std::string("--group-clustering") == argv[i]) {
      /* register group clustering sweep benchmarks */
      int dataSize = 20 * 1000 * 1000;
      int upperBound = 20 * 1000 * 1000;
      int cardinality = 5 * 1000 * 1000; // Change ~12 lines below too - must match
      std::ostringstream testName;
      std::string queryName = "group_clustering_sweep";
      testName << queryName << "/";
      testName << dataSize << " tuples";
      benchmark::RegisterBenchmark(testName.str(), group_clustering_sweep_Benchmark, dataSize,
                                   upperBound, cardinality, queryName)
          ->MeasureProcessCPUTime()
          ->UseRealTime()
          ->Apply([](benchmark::internal::Benchmark* b) {
            std::vector<int> thresholds = generateLogDistribution<int>(15, 1, 5 * 1000 * 1000);
            for(auto& value : thresholds) {
              b->Arg(value);
            }
          });
    } else if(std::string("--group-varyRatio") == argv[i]) {
      /* register group two section cardinality sweep benchmarks with varying ratio*/
      int dataSize = 200 * 1000 * 1000;
      int upperBound = 30 * 1000 * 1000;
      int cardinalitySectionOne = 100;
      int cardinalitySectionTwo = 30 * 1000 * 1000;
      std::ostringstream testName;
      std::string queryName = "section_ratio_sweep_step_cardinality_change";
      testName << queryName << "/";
      testName << dataSize << " tuples";
      benchmark::RegisterBenchmark(
          testName.str(), section_ratio_sweep_step_cardinality_change_Benchmark, dataSize,
          upperBound, cardinalitySectionOne, cardinalitySectionTwo, queryName)
          ->MeasureProcessCPUTime()
          ->UseRealTime()
          ->Apply([](benchmark::internal::Benchmark* b) {
            std::vector<float> thresholds = generateLinearDistribution<float>(21, 0, 1);
            for(auto& value : thresholds) {
              b->Arg(static_cast<int>(value * 100)); // Pass value to benchmark as an integer
            }
          });
    } else if(std::string("--group-cardinality-combinations") == argv[i]) {
      /* register group cardinality sweep combinations benchmarks */
      int dataSize = 20 * 1000 * 1000; // Change ~12 lines below too - must match
      int upperBound = 20 * 1000 * 1000;
      for(int queryIdx = 1; queryIdx <= 10; ++queryIdx) {
        std::ostringstream testName;
        std::string queryName = "group_cardinality_sweep";
        testName << queryName << "/";
        testName << groupCombinationQueryNames()[DATASETS::GROUP + queryIdx] << "/";
        testName << dataSize << " tuples";
        benchmark::RegisterBenchmark(testName.str(), group_cardinality_combinations_Benchmark,
                                     dataSize, upperBound, DATASETS::GROUP + queryIdx)
            ->MeasureProcessCPUTime()
            ->UseRealTime()
            ->Apply([](benchmark::internal::Benchmark* b) {
              std::vector<int> thresholds = generateLogDistribution<int>(15, 1, 20 * 1000 * 1000);
              for(auto& value : thresholds) {
                b->Arg(value);
              }
            });
      }
    }
  }

  // std::ostringstream testName1;
  // // auto const& queryName = etlQueriesNoTransformNames()[DATASETS::TPCH + query + dataSize + percentage];
  // auto const& queryName1 = etlQueriesTransformNames()[DATASETS::TPCH + 5];
  // testName1 << queryName1 << "/";
  // benchmark::RegisterBenchmark(testName1.str(), TPCH_Benchmark, DATASETS::TPCH + 5,
  //                             0, 0)
  //     ->MeasureProcessCPUTime()
  //     ->UseRealTime();
  // std::ostringstream testName2;
  // // auto const& queryName = etlQueriesNoTransformNames()[DATASETS::TPCH + query + dataSize + percentage];
  // auto const& queryName2 = etlQueriesTransformNames()[DATASETS::TPCH + 6];
  // testName2 << queryName2 << "/";
  // benchmark::RegisterBenchmark(testName2.str(), TPCH_Benchmark, DATASETS::TPCH + 6,
  //                             0, 0)
  //     ->MeasureProcessCPUTime()
  //     ->UseRealTime();
  /* register TPC-H benchmarks */
  for(int dataSize : std::vector<int>{1, 10, 100, 1000, 10000, 20000}) {
    for(int64_t blockSize :
        (BENCHMARK_STORAGE_BLOCK_SIZE
             ? std::vector<int64_t>{1 << 25, 1 << 26, 1 << 27, 1 << 28, 1 << 29, 1 << 30,
                                    std::numeric_limits<int32_t>::max()}
             : std::vector<int64_t>{DEFAULT_STORAGE_BLOCK_SIZE})) {
      for(int query : tpchQueriesToBenchmark) {
        // for (int percentage : {20, 40, 60, 80, 100}) {
        // for (int percentage : {20}) {
          std::ostringstream testName;
          // auto const& queryName = etlQueriesNoTransformNames()[DATASETS::TPCH + query + dataSize + percentage];
          auto const& queryName = etlQueriesTransformNames()[DATASETS::TPCH + query];
          testName << queryName << "/";
          testName << dataSize << "MB/";
          // testName << percentage << "pc";
          if(BENCHMARK_STORAGE_BLOCK_SIZE) {
            testName << "/bs:";
            testName << (blockSize >> 20) << "MB";
          }
          benchmark::RegisterBenchmark(testName.str(), TPCH_Benchmark, DATASETS::TPCH + query,
                                      dataSize, blockSize)
              ->MeasureProcessCPUTime()
              ->UseRealTime();
        // }
      }
    }
  }

  storageLibrary = USING_COORDINATOR_ENGINE ? librariesToTest[1] : librariesToTest[0];
  storageLibrary = "/mnt/e/University/Andrii/BOSSArrowStorageEngine/build/libBOSSArrowStorage.so";

  benchmark::Initialize(&argc, argv, ::benchmark::PrintDefaultHelp);
  benchmark::RunSpecifiedBenchmarks();

  releaseBOSSEngines();
}

int main(int argc, char** argv) {
  try {
    initAndRunBenchmarks(argc, argv);
  } catch(std::exception& e) {
    std::cerr << "caught exception in main: " << e.what() << std::endl;
    boss::evaluate("ResetEngines"_());
    return EXIT_FAILURE;
  } catch(...) {
    std::cerr << "unhandled exception." << std::endl;
    boss::evaluate("ResetEngines"_());
    return EXIT_FAILURE;
  }
  boss::evaluate("ResetEngines"_());
  return EXIT_SUCCESS;
}
