#include "dataGeneration.cpp"
#include "utilities.cpp"
#include <benchmark/benchmark.h>
#include <iostream>

using SpanArguments = boss::DefaultExpressionSystem::ExpressionSpanArguments;
using ComplexExpression = boss::DefaultExpressionSystem::ComplexExpression;
using ExpressionArguments = boss::ExpressionArguments;

void initStorageEngine_selectivity_sweep_uniform_dis(int dataSize) {
  static auto dataSet = std::string("selectivity_sweep_uniform_dis");

  if(latestDataSet == dataSet && latestDataSize == dataSize) {
    return;
  }

  resetStorageEngine();
  latestDataSet = dataSet;
  latestDataSize = dataSize;

  auto evalStorage = getEvaluateStorageLambda();
  auto checkForErrors = getCheckForErrorsLambda();

  checkForErrors(evalStorage("CreateTable"_("UNIFORM_DIS"_)));

  using intType = int64_t;
  auto keySpans = loadVectorIntoSpans(generateUniformDistribution<intType>(dataSize, 1, 10000));
  auto payloadSpans = loadVectorIntoSpans(generateUniformDistribution<intType>(dataSize, 1, 10000));

  ExpressionArguments keyColumn, payloadColumn, columns;
  keyColumn.emplace_back(ComplexExpression("List"_, {}, {}, std::move(keySpans)));
  payloadColumn.emplace_back(ComplexExpression("List"_, {}, {}, std::move(payloadSpans)));

  columns.emplace_back(ComplexExpression("key"_, {}, std::move(keyColumn), {}));
  columns.emplace_back(ComplexExpression("payload"_, {}, std::move(payloadColumn), {}));

  checkForErrors(evalStorage(
      "LoadDataTable"_("UNIFORM_DIS"_, ComplexExpression("Data"_, {}, std::move(columns), {}))));
}

void selectivity_sweep_uniform_dis_Benchmark(benchmark::State& state, int dataSize,
                                             const std::string& queryName) {
  initStorageEngine_selectivity_sweep_uniform_dis(dataSize);
  int threshold = state.range(0); // NOLINT

  boss::Expression query = "Select"_("UNIFORM_DIS"_, "Where"_("Greater"_(threshold, "key"_)));

  runBenchmark(state, queryName, query);
}

void initStorageEngine_randomness_sweep_sorted_dis(int dataSize, float percentageRandom) {
  static auto dataSet = std::string("randomness_sweep_sorted_dis");

  resetStorageEngine();
  latestDataSet = dataSet;
  latestDataSize = dataSize;

  auto evalStorage = getEvaluateStorageLambda();
  auto checkForErrors = getCheckForErrorsLambda();

  checkForErrors(evalStorage("CreateTable"_("PARTIALLY_SORTED_DIS"_)));

  using intType = int64_t;
  auto keySpans = loadVectorIntoSpans(
      generatePartiallySortedOneToOneHundred<intType>(dataSize, 10, percentageRandom));

  ExpressionArguments keyColumn, columns;
  keyColumn.emplace_back(ComplexExpression("List"_, {}, {}, std::move(keySpans)));

  columns.emplace_back(ComplexExpression("key"_, {}, std::move(keyColumn), {}));

  checkForErrors(evalStorage("LoadDataTable"_(
      "PARTIALLY_SORTED_DIS"_, ComplexExpression("Data"_, {}, std::move(columns), {}))));
}

void randomness_sweep_sorted_dis_Benchmark(benchmark::State& state, int dataSize,
                                           const std::string& queryName) {
  float percentageRandom = static_cast<float>(state.range(0)) / 100.0; // NOLINT
  initStorageEngine_randomness_sweep_sorted_dis(dataSize, percentageRandom);

  boss::Expression query = "Select"_("PARTIALLY_SORTED_DIS"_, "Where"_("Greater"_(51, "key"_)));

  runBenchmark(state, queryName, query);
}

void initStorageEngine_section_length_sweep_step_selectivity_change_(int dataSize, int sectionLength) {
  static auto dataSet = std::string("section_length_sweep_step_selectivity_change");

  resetStorageEngine();
  latestDataSet = dataSet;
  latestDataSize = dataSize;

  auto evalStorage = getEvaluateStorageLambda();
  auto checkForErrors = getCheckForErrorsLambda();

  checkForErrors(evalStorage("CreateTable"_("MULTI_SELECTIVITY"_)));

  using intType = int64_t;
  auto keySpans = loadVectorIntoSpans(
      generateStepChangeLowerBoundUniformDistVaryingLengthOfSection<intType>(dataSize, 1, 51, 100, sectionLength));
  auto payloadSpans = loadVectorIntoSpans(generateUniformDistribution<intType>(dataSize, 1, 10000));

  ExpressionArguments keyColumn, payloadColumn, columns;
  keyColumn.emplace_back(ComplexExpression("List"_, {}, {}, std::move(keySpans)));
  payloadColumn.emplace_back(ComplexExpression("List"_, {}, {}, std::move(payloadSpans)));

  columns.emplace_back(ComplexExpression("key"_, {}, std::move(keyColumn), {}));
  columns.emplace_back(ComplexExpression("payload"_, {}, std::move(payloadColumn), {}));

  checkForErrors(evalStorage(
      "LoadDataTable"_("MULTI_SELECTIVITY"_, ComplexExpression("Data"_, {}, std::move(columns), {}))));
}

void section_length_sweep_step_selectivity_change_Benchmark(benchmark::State& state, int dataSize,
                                             const std::string& queryName) {
  int sectionLength = state.range(0); // NOLINT
  initStorageEngine_section_length_sweep_step_selectivity_change_(dataSize, sectionLength);

  boss::Expression query = "Select"_("MULTI_SELECTIVITY"_, "Where"_("Greater"_(51, "key"_)));

  runBenchmark(state, queryName, query);
}

void initStorageEngine_section_ratio_sweep_step_selectivity_change_(int dataSize, float fractionSection1) {
  static auto dataSet = std::string("section_ratio_sweep_step_selectivity_change");

  resetStorageEngine();
  latestDataSet = dataSet;
  latestDataSize = dataSize;

  auto evalStorage = getEvaluateStorageLambda();
  auto checkForErrors = getCheckForErrorsLambda();

  checkForErrors(evalStorage("CreateTable"_("MULTI_SELECTIVITY"_)));

  using intType = int64_t;
  auto keySpans = loadVectorIntoSpans(
      generateStepChangeLowerBoundUniformDistVaryingPercentageSectionOne<intType>(dataSize, 1, 51, 100, 10, fractionSection1));
  auto payloadSpans = loadVectorIntoSpans(generateUniformDistribution<intType>(dataSize, 1, 10000));

  ExpressionArguments keyColumn, payloadColumn, columns;
  keyColumn.emplace_back(ComplexExpression("List"_, {}, {}, std::move(keySpans)));
  payloadColumn.emplace_back(ComplexExpression("List"_, {}, {}, std::move(payloadSpans)));

  columns.emplace_back(ComplexExpression("key"_, {}, std::move(keyColumn), {}));
  columns.emplace_back(ComplexExpression("payload"_, {}, std::move(payloadColumn), {}));

  checkForErrors(evalStorage(
      "LoadDataTable"_("MULTI_SELECTIVITY"_, ComplexExpression("Data"_, {}, std::move(columns), {}))));
}

void section_ratio_sweep_step_selectivity_change_Benchmark(benchmark::State& state, int dataSize,
                                                            const std::string& queryName) {
  float fractionSection1 = static_cast<float>(state.range(0)) / 100.0; // NOLINT
  initStorageEngine_section_ratio_sweep_step_selectivity_change_(dataSize, fractionSection1);

  boss::Expression query = "Select"_("MULTI_SELECTIVITY"_, "Where"_("Greater"_(51, "key"_)));

  runBenchmark(state, queryName, query);
}