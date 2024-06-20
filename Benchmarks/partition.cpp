#include "dataGeneration.cpp"
#include "utilities.cpp"
#include <benchmark/benchmark.h>

#include <iostream>
#include <limits>
#include <unistd.h>

// #define CHECK_PARTITION_RESULT_CORRECTNESS
// #define PRINT_PARTITION_STATS

using SpanArguments = boss::DefaultExpressionSystem::ExpressionSpanArguments;
using ComplexExpression = boss::DefaultExpressionSystem::ComplexExpression;
using ExpressionArguments = boss::ExpressionArguments;
using Expression = boss::expressions::Expression;

void checkPartitionResultCorrectness(boss::expressions::Expression&& expr);

inline uint64_t l2cacheSize() { return sysconf(_SC_LEVEL2_CACHE_SIZE); }

void initStorageEngine_partition_clustering_sweep(int dataSize, int upperBound, int cardinality,
                                                  int spreadInCluster) {
  static auto dataSet = std::string("partition_clustering_sweep");

  resetStorageEngine();
  latestDataSet = dataSet;
  latestDataSize = dataSize;

  auto evalStorage = getEvaluateStorageLambda();
  auto checkForErrors = getCheckForErrorsLambda();

  checkForErrors(evalStorage("CreateTable"_("CLUSTERED_DIS_1"_)));
  checkForErrors(evalStorage("CreateTable"_("CLUSTERED_DIS_2"_)));

  using intType = int64_t;
  auto keySpans1 =
      loadVectorIntoSpans(generateUniformDistributionWithSetCardinalityClustered<intType>(
          dataSize, upperBound, cardinality, spreadInCluster, 1));
  auto keySpans2 =
      loadVectorIntoSpans(generateUniformDistributionWithSetCardinalityClustered<intType>(
          dataSize, upperBound, cardinality, spreadInCluster, 2));

  ExpressionArguments keyColumn1, columns1;
  keyColumn1.emplace_back(ComplexExpression("List"_, {}, {}, std::move(keySpans1)));
  columns1.emplace_back(ComplexExpression("key1"_, {}, std::move(keyColumn1), {}));

  ExpressionArguments keyColumn2, columns2;
  keyColumn2.emplace_back(ComplexExpression("List"_, {}, {}, std::move(keySpans2)));
  columns2.emplace_back(ComplexExpression("key2"_, {}, std::move(keyColumn2), {}));

  checkForErrors(evalStorage("LoadDataTable"_(
      "CLUSTERED_DIS_1"_, ComplexExpression("Data"_, {}, std::move(columns1), {}))));
  checkForErrors(evalStorage("LoadDataTable"_(
      "CLUSTERED_DIS_2"_, ComplexExpression("Data"_, {}, std::move(columns2), {}))));
}

void partition_clustering_sweep_Benchmark(benchmark::State& state, int dataSize, int upperBound,
                                          int cardinality, const std::string& queryName) {
  int spreadInCluster = static_cast<int>(state.range(0));
  initStorageEngine_partition_clustering_sweep(dataSize, upperBound, cardinality, spreadInCluster);

  boss::Expression query =
      "Join"_("CLUSTERED_DIS_1"_, "CLUSTERED_DIS_2"_, "Where"_("Equal"_("key1"_, "key2"_)));

#ifdef CHECK_PARTITION_RESULT_CORRECTNESS
  auto eval = getEvaluateLambda();
  auto testResult = eval(utilities::shallowCopy(std::get<boss::ComplexExpression>(query)));
  checkPartitionResultCorrectness(std::move(testResult));
  std::cout << "Join partitions are valid" << std::endl;
#endif

  runBenchmark(state, queryName, query);
}

void initStorageEngine_partition_cardinality_sweep(int dataSize, int upperBound, int cardinality) {
  static auto dataSet = std::string("partition_cardinality_sweep");

  resetStorageEngine();
  latestDataSet = dataSet;
  latestDataSize = dataSize;

  auto evalStorage = getEvaluateStorageLambda();
  auto checkForErrors = getCheckForErrorsLambda();

  checkForErrors(evalStorage("CreateTable"_("FIXED_UPPER_BOUND_DIS_1"_)));
  checkForErrors(evalStorage("CreateTable"_("FIXED_UPPER_BOUND_DIS_2"_)));

  using intType = int64_t;
  auto keySpans1 = loadVectorIntoSpans(
      generateUniformDistributionWithSetCardinality<intType>(dataSize, upperBound, cardinality, 1));
  auto keySpans2 = loadVectorIntoSpans(
      generateUniformDistributionWithSetCardinality<intType>(dataSize, upperBound, cardinality, 2));

  ExpressionArguments keyColumn1, columns1;
  keyColumn1.emplace_back(ComplexExpression("List"_, {}, {}, std::move(keySpans1)));
  columns1.emplace_back(ComplexExpression("key1"_, {}, std::move(keyColumn1), {}));

  ExpressionArguments keyColumn2, columns2;
  keyColumn2.emplace_back(ComplexExpression("List"_, {}, {}, std::move(keySpans2)));
  columns2.emplace_back(ComplexExpression("key2"_, {}, std::move(keyColumn2), {}));

  checkForErrors(evalStorage("LoadDataTable"_(
      "FIXED_UPPER_BOUND_DIS_1"_, ComplexExpression("Data"_, {}, std::move(columns1), {}))));
  checkForErrors(evalStorage("LoadDataTable"_(
      "FIXED_UPPER_BOUND_DIS_2"_, ComplexExpression("Data"_, {}, std::move(columns2), {}))));
}

void partition_cardinality_sweep_Benchmark(benchmark::State& state, int dataSize, int upperBound,
                                           const std::string& queryName) {
  int cardinality = static_cast<int>(state.range(0));
  initStorageEngine_partition_cardinality_sweep(dataSize, upperBound, cardinality);

  boss::Expression query = "Join"_("FIXED_UPPER_BOUND_DIS_1"_, "FIXED_UPPER_BOUND_DIS_2"_,
                                   "Where"_("Equal"_("key1"_, "key2"_)));

#ifdef CHECK_PARTITION_RESULT_CORRECTNESS
  auto eval = getEvaluateLambda();
  auto testResult = eval(utilities::shallowCopy(std::get<boss::ComplexExpression>(query)));
  checkPartitionResultCorrectness(std::move(testResult));
  std::cout << "Join partitions are valid" << std::endl;
#endif

  runBenchmark(state, queryName, query);
}

void checkPartitionResultCorrectness(Expression&& e) {
  auto expr = std::get<ComplexExpression>(std::move(e));
  auto [head, unused_, dynamics, spans] = std::move(expr).decompose();
  assert(head.getName() == "Join");
  auto tableOne = std::move(dynamics.at(0));
  auto tableTwo = std::move(dynamics.at(1));

  auto [tableOneHead, unused1, tableOneDynamics, unused2] =
      std::move(get<ComplexExpression>(tableOne)).decompose();
  auto [tableTwoHead, unused3, tableTwoDynamics, unused4] =
      std::move(get<ComplexExpression>(tableTwo)).decompose();

  assert(tableOneHead.getName() == "RadixPartition");
  assert(tableTwoHead.getName() == "RadixPartition");

  int maxBuildElemsPerPartition = -1;

  auto getKeysMinMaxFromPartition =
      [&maxBuildElemsPerPartition](ComplexExpression&& expr,
                                   bool checkSize) -> std::pair<int64_t, int64_t> {
#ifdef PRINT_PARTITION_STATS
    std::cout << "Partition: " << expr << std::endl;
#endif
    auto [partitionHead, unused5, partitionDynamics, unused6] = std::move(expr).decompose();
    assert(partitionHead.getName() == "Partition");
    auto keys = std::move(partitionDynamics.at(0));
    auto [keysHead, unused7, keysDynamics, unused8] =
        std::move(get<ComplexExpression>(keys)).decompose();
    auto keyList = get<ComplexExpression>(std::move(keysDynamics.at(0)));

    auto [keyListHead, unused9, unused10, keyListSpans] = std::move(keyList).decompose();
    assert(keyListHead.getName() == "List");
    auto indexes = std::move(get<ComplexExpression>(partitionDynamics.at(1)));
    auto [indexesHead, unused11, unused12, indexesSpans] = std::move(indexes).decompose();
    assert(indexesHead.getName() == "Indexes");

    if(maxBuildElemsPerPartition == -1) {
      std::visit(
          [&maxBuildElemsPerPartition]<typename T>(boss::Span<T>& /*unused*/) {
            if constexpr(std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>) {
              maxBuildElemsPerPartition = static_cast<double>(l2cacheSize()) / sizeof(T);
            } else {
              throw std::runtime_error("span type not in Partition correctness check");
            }
          },
          keyListSpans.at(0));
    }

    int64_t keyMin = std::numeric_limits<int64_t>::max();
    int64_t keyMax = std::numeric_limits<int64_t>::min();
    size_t numKeys = 0;
    size_t numIndexes = 0;

    assert(keyListSpans.size() == indexesSpans.size());
    for(auto& span : keyListSpans) {
      std::visit(
          [&keyMin, &keyMax, &numKeys]<typename T>(boss::Span<T>& typedSpan) {
            if constexpr(std::is_same_v<T, int32_t> || std::is_same_v<T, int64_t>) {
              numKeys += typedSpan.size();
              for(const auto& value : typedSpan) {
                keyMin = std::min(keyMin, static_cast<int64_t>(value));
                keyMax = std::max(keyMax, static_cast<int64_t>(value));
              }
            } else {
              throw std::runtime_error("span type not in Partition correctness check");
            }
          },
          span);
    }

    for(auto& span : indexesSpans) {
      std::visit(
          [&numIndexes]<typename T>(boss::Span<T>& typedSpan) {
            if constexpr(std::is_same_v<T, int32_t>) {
              numIndexes += typedSpan.size();
            } else {
              throw std::runtime_error("indexes in Partition should be int32_t");
            }
          },
          span);
    }

    assert(numKeys == numIndexes);
    if(checkSize) {
      assert(numKeys <= static_cast<size_t>(maxBuildElemsPerPartition));
    }

#ifdef PRINT_PARTITION_STATS
    std::cout << "[keyMin,keyMax] = [" << keyMin << "," << keyMax << "]" << std::endl;
    std::cout << "[numKeys,numIndexes] = [" << numKeys << "," << numIndexes << "]" << std::endl;
    std::cout << "[numKeySpans,numIndexSpans] = [" << keyListSpans.size() << ","
              << indexesSpans.size() << "]" << std::endl;
    std::cout << std::endl;
#endif

    return {keyMin, keyMax};
  };

  assert(tableOneDynamics.size() == tableTwoDynamics.size());
  auto [prevKeyMin1, prevKeyMax1] =
      getKeysMinMaxFromPartition(std::move(get<ComplexExpression>(tableOneDynamics.at(1))), true);
  auto [prevKeyMin2, prevKeyMax2] =
      getKeysMinMaxFromPartition(std::move(get<ComplexExpression>(tableTwoDynamics.at(1))), false);
  int64_t prevPartitionMax = std::max(prevKeyMax1, prevKeyMax2);
  int64_t partitionMin, partitionMax;

  for(size_t i = 2; i < tableOneDynamics.size(); ++i) {
    auto [keyMin1, keyMax1] =
        getKeysMinMaxFromPartition(std::move(get<ComplexExpression>(tableOneDynamics.at(i))), true);
    auto [keyMin2, keyMax2] = getKeysMinMaxFromPartition(
        std::move(get<ComplexExpression>(tableTwoDynamics.at(i))), false);
    partitionMin = std::min(keyMin1, keyMin2);
    partitionMax = std::max(keyMax1, keyMax2);
    assert(partitionMin > prevPartitionMax);
    prevPartitionMax = partitionMax;
  }
}