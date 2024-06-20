#include "dataGeneration.cpp"
#include "utilities.cpp"
#include <benchmark/benchmark.h>

using SpanArguments = boss::DefaultExpressionSystem::ExpressionSpanArguments;
using ComplexExpression = boss::DefaultExpressionSystem::ComplexExpression;
using ExpressionArguments = boss::ExpressionArguments;
using Expression = boss::expressions::Expression;

enum GROUP_COMBINATION_QUERIES {
  KEY4_PAY4 = 1,
  KEY4_PAY8,
  KEY8_PAY4,
  KEY8_PAY8,
  KEY8_PAY16,
  KEY8_PAY24,
  KEY8_PAY32,
  KEY8_PAY40,
  KEY4_PAY12,
  KEY4_PAY20
};

void initStorageEngine_group_cardinality_sweep(int dataSize, int upperBound, int cardinality) {
  static auto dataSet = std::string("group_cardinality_sweep");

  resetStorageEngine();
  latestDataSet = dataSet;
  latestDataSize = dataSize;

  setCardinalityEnvironmentVariable(cardinality);

  auto evalStorage = getEvaluateStorageLambda();
  auto checkForErrors = getCheckForErrorsLambda();

  checkForErrors(evalStorage("CreateTable"_("FIXED_UPPER_BOUND_DIS"_)));

  auto keySpans = loadVectorIntoSpans(
      generateUniformDistributionWithSetCardinality<int64_t>(dataSize, upperBound, cardinality));
  auto payloadSpans =
      loadVectorIntoSpans(generateUniformDistribution<int64_t>(dataSize, 1, upperBound));

  ExpressionArguments keyColumn, payloadColumn, columns;
  keyColumn.emplace_back(ComplexExpression("List"_, {}, {}, std::move(keySpans)));
  payloadColumn.emplace_back(ComplexExpression("List"_, {}, {}, std::move(payloadSpans)));

  columns.emplace_back(ComplexExpression("key"_, {}, std::move(keyColumn), {}));
  columns.emplace_back(ComplexExpression("payload"_, {}, std::move(payloadColumn), {}));

  checkForErrors(evalStorage("LoadDataTable"_(
      "FIXED_UPPER_BOUND_DIS"_, ComplexExpression("Data"_, {}, std::move(columns), {}))));
}

void group_cardinality_sweep_Benchmark(benchmark::State& state, int dataSize, int upperBound,
                                       const std::string& queryName) {
  int cardinality = static_cast<int>(state.range(0));
  initStorageEngine_group_cardinality_sweep(dataSize, upperBound, cardinality);

  boss::Expression query =
      "Group"_("FIXED_UPPER_BOUND_DIS"_, "By"_("key"_), "As"_("max_payload"_, "Max"_("payload"_)));

  runBenchmark(state, queryName, query);
}

void initStorageEngine_group_clustering_sweep(int dataSize, int upperBound, int cardinality,
                                              int spreadInCluster) {
  static auto dataSet = std::string("group_clustering_sweep");

  setCardinalityEnvironmentVariable(cardinality);

  resetStorageEngine();
  latestDataSet = dataSet;
  latestDataSize = dataSize;

  auto evalStorage = getEvaluateStorageLambda();
  auto checkForErrors = getCheckForErrorsLambda();

  checkForErrors(evalStorage("CreateTable"_("CLUSTERED_DIS"_)));

  auto keySpans =
      loadVectorIntoSpans(generateUniformDistributionWithSetCardinalityClustered<int64_t>(
          dataSize, upperBound, cardinality, spreadInCluster));
  auto payloadSpans =
      loadVectorIntoSpans(generateUniformDistribution<int64_t>(dataSize, 1, upperBound));

  ExpressionArguments keyColumn, payloadColumn, columns;
  keyColumn.emplace_back(ComplexExpression("List"_, {}, {}, std::move(keySpans)));
  payloadColumn.emplace_back(ComplexExpression("List"_, {}, {}, std::move(payloadSpans)));

  columns.emplace_back(ComplexExpression("key"_, {}, std::move(keyColumn), {}));
  columns.emplace_back(ComplexExpression("payload"_, {}, std::move(payloadColumn), {}));

  checkForErrors(evalStorage(
      "LoadDataTable"_("CLUSTERED_DIS"_, ComplexExpression("Data"_, {}, std::move(columns), {}))));
}

void group_clustering_sweep_Benchmark(benchmark::State& state, int dataSize, int upperBound,
                                      int cardinality, const std::string& queryName) {
  int spreadInCluster = static_cast<int>(state.range(0));
  initStorageEngine_group_clustering_sweep(dataSize, upperBound, cardinality, spreadInCluster);

  boss::Expression query =
      "Group"_("CLUSTERED_DIS"_, "By"_("key"_), "As"_("max_payload"_, "Max"_("payload"_)));

  runBenchmark(state, queryName, query);
}

void initStorageEngine_section_ratio_sweep_step_cardinality_change(int dataSize, int upperBound, int cardinalitySectionOne,
                                                                   int cardinalitySectionTwo, float fractionSection1) {
  static auto dataSet = std::string("section_ratio_sweep_step_cardinality_change");

  resetStorageEngine();
  latestDataSet = dataSet;
  latestDataSize = dataSize;

  setCardinalityEnvironmentVariable(cardinalitySectionOne + cardinalitySectionTwo);

  auto evalStorage = getEvaluateStorageLambda();
  auto checkForErrors = getCheckForErrorsLambda();

  checkForErrors(evalStorage("CreateTable"_("MULTI_CARDINALITY"_)));

  auto keySpans = loadVectorIntoSpans(
      generateUniformDistWithTwoCardinalitySectionsOfDifferentLengths<int64_t>(dataSize, upperBound, cardinalitySectionOne,
                                                                               cardinalitySectionTwo, fractionSection1));
  auto payloadSpans =
      loadVectorIntoSpans(generateUniformDistribution<int64_t>(dataSize, 1, upperBound));

  ExpressionArguments keyColumn, payloadColumn, columns;
  keyColumn.emplace_back(ComplexExpression("List"_, {}, {}, std::move(keySpans)));
  payloadColumn.emplace_back(ComplexExpression("List"_, {}, {}, std::move(payloadSpans)));

  columns.emplace_back(ComplexExpression("key"_, {}, std::move(keyColumn), {}));
  columns.emplace_back(ComplexExpression("payload"_, {}, std::move(payloadColumn), {}));

  checkForErrors(evalStorage("LoadDataTable"_(
      "MULTI_CARDINALITY"_, ComplexExpression("Data"_, {}, std::move(columns), {}))));
}

void section_ratio_sweep_step_cardinality_change_Benchmark(benchmark::State& state, int dataSize,
                                                           int upperBound, int cardinalitySectionOne, int cardinalitySectionTwo,
                                                           const std::string& queryName) {
  float fractionSection1 = static_cast<float>(state.range(0)) / 100.0; // NOLINT
  initStorageEngine_section_ratio_sweep_step_cardinality_change(dataSize, upperBound, cardinalitySectionOne,
                                                                cardinalitySectionTwo, fractionSection1);

  boss::Expression query =
      "Group"_("MULTI_CARDINALITY"_, "By"_("key"_), "As"_("max_payload"_, "Max"_("payload"_)));

  runBenchmark(state, queryName, query);
}

static auto& groupCombinationQueryNames() {
  static std::map<int, std::string> names;
  if(names.empty()) {
    names.try_emplace(static_cast<int>(DATASETS::GROUP) +
                          static_cast<int>(GROUP_COMBINATION_QUERIES::KEY4_PAY4),
                      "KEY4_PAY4");
    names.try_emplace(static_cast<int>(DATASETS::GROUP) +
                          static_cast<int>(GROUP_COMBINATION_QUERIES::KEY4_PAY8),
                      "KEY4_PAY8");
    names.try_emplace(static_cast<int>(DATASETS::GROUP) +
                          static_cast<int>(GROUP_COMBINATION_QUERIES::KEY8_PAY4),
                      "KEY8_PAY4");
    names.try_emplace(static_cast<int>(DATASETS::GROUP) +
                          static_cast<int>(GROUP_COMBINATION_QUERIES::KEY8_PAY8),
                      "KEY8_PAY8");
    names.try_emplace(static_cast<int>(DATASETS::GROUP) +
                          static_cast<int>(GROUP_COMBINATION_QUERIES::KEY8_PAY16),
                      "KEY8_PAY16");
    names.try_emplace(static_cast<int>(DATASETS::GROUP) +
                          static_cast<int>(GROUP_COMBINATION_QUERIES::KEY8_PAY24),
                      "KEY8_PAY24");
    names.try_emplace(static_cast<int>(DATASETS::GROUP) +
                          static_cast<int>(GROUP_COMBINATION_QUERIES::KEY8_PAY32),
                      "KEY8_PAY32");
    names.try_emplace(static_cast<int>(DATASETS::GROUP) +
                          static_cast<int>(GROUP_COMBINATION_QUERIES::KEY8_PAY40),
                      "KEY8_PAY40");
    names.try_emplace(static_cast<int>(DATASETS::GROUP) +
                          static_cast<int>(GROUP_COMBINATION_QUERIES::KEY4_PAY12),
                      "KEY4_PAY12");
    names.try_emplace(static_cast<int>(DATASETS::GROUP) +
                          static_cast<int>(GROUP_COMBINATION_QUERIES::KEY4_PAY20),
                      "KEY4_PAY20");
  }
  return names;
}

auto& groupCombinationQueries() {
  // Queries are Expressions and therefore cannot be just a table i.e. a Symbol
  static std::map<int, boss::Expression> queries;
  if(queries.empty()) {
    queries.try_emplace(static_cast<int>(DATASETS::GROUP) +
                            static_cast<int>(GROUP_COMBINATION_QUERIES::KEY4_PAY4),
                        "Group"_("FIXED_UPPER_BOUND_DIS"_, "By"_("key32"_),
                                 "As"_("max_payload32"_, "Max"_("payload32"_))));
    queries.try_emplace(static_cast<int>(DATASETS::GROUP) +
                            static_cast<int>(GROUP_COMBINATION_QUERIES::KEY4_PAY8),
                        "Group"_("FIXED_UPPER_BOUND_DIS"_, "By"_("key32"_),
                                 "As"_("max_payload64"_, "Max"_("payload64"_))));
    queries.try_emplace(static_cast<int>(DATASETS::GROUP) +
                            static_cast<int>(GROUP_COMBINATION_QUERIES::KEY8_PAY4),
                        "Group"_("FIXED_UPPER_BOUND_DIS"_, "By"_("key64"_),
                                 "As"_("max_payload32"_, "Max"_("payload32"_))));
    queries.try_emplace(static_cast<int>(DATASETS::GROUP) +
                            static_cast<int>(GROUP_COMBINATION_QUERIES::KEY8_PAY8),
                        "Group"_("FIXED_UPPER_BOUND_DIS"_, "By"_("key64"_),
                                 "As"_("max_payload64"_, "Max"_("payload64"_))));
    queries.try_emplace(static_cast<int>(DATASETS::GROUP) +
                            static_cast<int>(GROUP_COMBINATION_QUERIES::KEY8_PAY16),
                        "Group"_("FIXED_UPPER_BOUND_DIS"_, "By"_("key64"_),
                                 "As"_("max_payload64"_, "Max"_("payload64"_), "min_payload64"_,
                                       "Min"_("payload64"_))));
    queries.try_emplace(
        static_cast<int>(DATASETS::GROUP) + static_cast<int>(GROUP_COMBINATION_QUERIES::KEY8_PAY24),
        "Group"_("FIXED_UPPER_BOUND_DIS"_, "By"_("key64"_),
                 "As"_("max_payload64"_, "Max"_("payload64"_), "sum_payload64"_,
                       "Sum"_("payload64"_), "min_payload64"_, "Min"_("payload64"_))));
    queries.try_emplace(static_cast<int>(DATASETS::GROUP) +
                            static_cast<int>(GROUP_COMBINATION_QUERIES::KEY8_PAY32),
                        "Group"_("FIXED_UPPER_BOUND_DIS"_, "By"_("key64"_),
                                 "As"_("max_payload64"_, "Max"_("payload64"_), "sum_payload64"_,
                                       "Sum"_("payload64"_), "min_payload64"_, "Min"_("payload64"_),
                                       "max_payload64_2"_, "Max"_("payload64_2"_))));
    queries.try_emplace(static_cast<int>(DATASETS::GROUP) +
                            static_cast<int>(GROUP_COMBINATION_QUERIES::KEY8_PAY40),
                        "Group"_("FIXED_UPPER_BOUND_DIS"_, "By"_("key64"_),
                                 "As"_("max_payload64"_, "Max"_("payload64"_), "sum_payload64"_,
                                       "Sum"_("payload64"_), "min_payload64"_, "Min"_("payload64"_),
                                       "max_payload64_2"_, "Max"_("payload64_2"_),
                                       "min_payload64_2"_, "Min"_("payload64_2"_))));
    queries.try_emplace(
        static_cast<int>(DATASETS::GROUP) + static_cast<int>(GROUP_COMBINATION_QUERIES::KEY4_PAY12),
        "Group"_("FIXED_UPPER_BOUND_DIS"_, "By"_("key32"_),
                 "As"_("max_payload32"_, "Max"_("payload32"_), "sum_payload32"_,
                       "Sum"_("payload32"_), "min_payload32"_, "Min"_("payload32"_))));
    queries.try_emplace(static_cast<int>(DATASETS::GROUP) +
                            static_cast<int>(GROUP_COMBINATION_QUERIES::KEY4_PAY20),
                        "Group"_("FIXED_UPPER_BOUND_DIS"_, "By"_("key32"_),
                                 "As"_("max_payload32"_, "Max"_("payload32"_), "sum_payload32"_,
                                       "Sum"_("payload32"_), "min_payload32"_, "Min"_("payload32"_),
                                       "max_payload64_2"_, "Max"_("payload64_2"_))));
  }
  return queries;
}

void initStorageEngine_group_cardinality_sweep_combinations(int dataSize, int upperBound,
                                                            int cardinality) {
  static auto dataSet = std::string("group_cardinality_sweep_different_types");

  resetStorageEngine();
  latestDataSet = dataSet;
  latestDataSize = dataSize;

  setCardinalityEnvironmentVariable(cardinality);

  auto evalStorage = getEvaluateStorageLambda();
  auto checkForErrors = getCheckForErrorsLambda();

  checkForErrors(evalStorage("CreateTable"_("FIXED_UPPER_BOUND_DIS"_)));

  auto keySpans32 = loadVectorIntoSpans(
      generateUniformDistributionWithSetCardinality<int32_t>(dataSize, upperBound, cardinality));
  auto keySpans64 = loadVectorIntoSpans(
      generateUniformDistributionWithSetCardinality<int64_t>(dataSize, upperBound, cardinality));
  auto payloadSpans32 =
      loadVectorIntoSpans(generateUniformDistribution<int32_t>(dataSize, 1, upperBound));
  auto payloadSpans64 =
      loadVectorIntoSpans(generateUniformDistribution<int64_t>(dataSize, 1, upperBound));
  auto payloadSpans64_2 =
      loadVectorIntoSpans(generateUniformDistribution<int64_t>(dataSize, 1, upperBound, 2));

  ExpressionArguments keyColumn32, keyColumn64, payloadColumn32, payloadColumn64, payloadColumn64_2,
      columns;
  keyColumn32.emplace_back(ComplexExpression("List"_, {}, {}, std::move(keySpans32)));
  keyColumn64.emplace_back(ComplexExpression("List"_, {}, {}, std::move(keySpans64)));
  payloadColumn32.emplace_back(ComplexExpression("List"_, {}, {}, std::move(payloadSpans32)));
  payloadColumn64.emplace_back(ComplexExpression("List"_, {}, {}, std::move(payloadSpans64)));
  payloadColumn64_2.emplace_back(ComplexExpression("List"_, {}, {}, std::move(payloadSpans64_2)));

  columns.emplace_back(ComplexExpression("key32"_, {}, std::move(keyColumn32), {}));
  columns.emplace_back(ComplexExpression("key64"_, {}, std::move(keyColumn64), {}));
  columns.emplace_back(ComplexExpression("payload32"_, {}, std::move(payloadColumn32), {}));
  columns.emplace_back(ComplexExpression("payload64"_, {}, std::move(payloadColumn64), {}));
  columns.emplace_back(ComplexExpression("payload64_2"_, {}, std::move(payloadColumn64_2), {}));

  checkForErrors(evalStorage("LoadDataTable"_(
      "FIXED_UPPER_BOUND_DIS"_, ComplexExpression("Data"_, {}, std::move(columns), {}))));
}

void group_cardinality_combinations_Benchmark(benchmark::State& state, int dataSize, int upperBound,
                                              int queryIdx) {
  int cardinality = static_cast<int>(state.range(0));
  initStorageEngine_group_cardinality_sweep_combinations(dataSize, upperBound, cardinality);

  auto const& queryName = groupCombinationQueryNames().find(queryIdx)->second;
  auto const& query = groupCombinationQueries().find(queryIdx)->second;

  runBenchmark(state, queryName, query);
}