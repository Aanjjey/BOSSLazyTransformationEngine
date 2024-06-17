#include "config.hpp"
#include "dataGeneration.cpp"
#include "utilities.cpp"
#include <benchmark/benchmark.h>
#include <iostream>

using SpanArguments = boss::DefaultExpressionSystem::ExpressionSpanArguments;
using SpanArgument = boss::DefaultExpressionSystem::ExpressionSpanArgument;
using ComplexExpression = boss::DefaultExpressionSystem::ComplexExpression;
using ExpressionArguments = boss::ExpressionArguments;

enum TPCH_QUERIES {
  TPCH_Q1 = 1,
  TPCH_Q3 = 3,
  TPCH_Q6 = 6,
  TPCH_Q9 = 9,
  TPCH_Q18 = 18,
  TPCH_Q3_FIRST_JOIN_ONLY = 31,
  TPCH_Q3_NOTOP,
  TPCH_Q9_SPLIT_TWO_KEYS,
  TPCH_Q18_SUBQUERY,
  TPCH_Q18_NOTOP,
};

enum ETL_TRANSFORMATIONS {
  LINE_VIEW_TRANSFORM = 5,
  BUTTERFLY_TRANSFORM = 6
};

enum ETL_QUERIES {
  LINE_VIEW_03 = 100000,
  LINE_VIEW_04 = 200000,
  BUTTERFLY_1_VIEW_12 = 300000,
  BUTTERFLY_1_VIEW_13 = 400000
};
enum SF {
  SF_1 = 1,
  SF_10 = 10,
  SF_100 = 100,
  SF_1000 = 1000,
  SF_10000 = 10000
};
enum PERCENTAGE {
  PERCENT_20 = 20,
  PERCENT_40 = 40,
  PERCENT_60 = 60,
  PERCENT_80 = 80,
  PERCENT_100 = 100
};
enum TRANSFORMATION {
  LINE = 1,
  BUTTERFLY = 2
};

void initStorageEngine_TPCH(int dataSize, int blockSize) {
  static auto dataSet = std::string("TPCH");

  if(latestDataSet == dataSet && latestDataSize == dataSize && latestBlockSize == blockSize) {
    return;
  }

  resetStorageEngine();
  latestDataSet = dataSet;
  latestDataSize = dataSize;
  latestBlockSize = blockSize;

  auto evalStorage = getEvaluateStorageLambda();
  auto checkForErrors = getCheckForErrorsLambda();

#if FALSE
  checkForErrors(eval("Set"_("LoadToMemoryMappedFiles"_, !DISABLE_MMAP_CACHE)));
  checkForErrors(eval("Set"_("UseAutoDictionaryEncoding"_, !DISABLE_AUTO_DICTIONARY_ENCODING)));
  checkForErrors(eval("Set"_("AllStringColumnsAsIntegers"_, ALL_STRINGS_AS_INTEGERS)));
#endif

  if(blockSize > 0) {
    checkForErrors(evalStorage("Set"_("FileLoadingBlockSize"_, blockSize)));
  }

  checkForErrors(evalStorage("CreateTable"_(
      "LINEITEM"_, "l_orderkey"_, "l_partkey"_, "l_suppkey"_, "l_linenumber"_, "l_quantity"_,
      "l_extendedprice"_, "l_discount"_, "l_tax"_, "l_returnflag"_, "l_linestatus"_, "l_shipdate"_,
      "l_commitdate"_, "l_receiptdate"_, "l_shipinstruct"_, "l_shipmode"_, "l_comment"_)));

  checkForErrors(evalStorage("CreateTable"_("REGION"_, "r_regionkey"_, "r_name"_, "r_comment"_)));

  checkForErrors(evalStorage(
      "CreateTable"_("NATION"_, "n_nationkey"_, "n_name"_, "n_regionkey"_, "n_comment"_)));

  checkForErrors(
      evalStorage("CreateTable"_("PART"_, "p_partkey"_, "p_name"_, "p_mfgr"_, "p_brand"_, "p_type"_,
                                 "p_size"_, "p_container"_, "p_retailprice"_, "p_comment"_)));

  checkForErrors(
      evalStorage("CreateTable"_("SUPPLIER"_, "s_suppkey"_, "s_name"_, "s_address"_, "s_nationkey"_,
                                 "s_phone"_, "s_acctbal"_, "s_comment"_)));

  checkForErrors(evalStorage("CreateTable"_("PARTSUPP"_, "ps_partkey"_, "ps_suppkey"_,
                                            "ps_availqty"_, "ps_supplycost"_, "ps_comment"_)));

  checkForErrors(
      evalStorage("CreateTable"_("CUSTOMER"_, "c_custkey"_, "c_name"_, "c_address"_, "c_nationkey"_,
                                 "c_phone"_, "c_acctbal"_, "c_mktsegment"_, "c_comment"_)));

  checkForErrors(evalStorage("CreateTable"_(
      "ORDERS"_, "o_orderkey"_, "o_custkey"_, "o_orderstatus"_, "o_totalprice"_, "o_orderdate"_,
      "o_orderpriority"_, "o_clerk"_, "o_shippriority"_, "o_comment"_)));

  auto filenamesAndTables = std::vector<std::pair<std::string, boss::Symbol>>{
      {"lineitem", "LINEITEM"_}, {"region", "REGION"_},     {"nation", "NATION"_},
      {"part", "PART"_},         {"supplier", "SUPPLIER"_}, {"partsupp", "PARTSUPP"_},
      {"customer", "CUSTOMER"_}, {"orders", "ORDERS"_}};

  for(auto const& [filename, table] : filenamesAndTables) {
    std::string path =
        tpch_filePath_prefix + "data/tpch_" + std::to_string(dataSize) + "MB/" + filename + ".tbl";
    checkForErrors(evalStorage("Load"_(table, path)));
  }

  if(ENABLE_CONSTRAINTS) {
    // primary key constraints
    checkForErrors(evalStorage("AddConstraint"_("PART"_, "PrimaryKey"_("p_partkey"_))));
    checkForErrors(evalStorage("AddConstraint"_("SUPPLIER"_, "PrimaryKey"_("s_suppkey"_))));
    checkForErrors(
        evalStorage("AddConstraint"_("PARTSUPP"_, "PrimaryKey"_("ps_partkey"_, "ps_suppkey"_))));
    checkForErrors(evalStorage("AddConstraint"_("CUSTOMER"_, "PrimaryKey"_("c_custkey"_))));
    checkForErrors(evalStorage("AddConstraint"_("ORDERS"_, "PrimaryKey"_("o_orderkey"_))));
    checkForErrors(
        evalStorage("AddConstraint"_("LINEITEM"_, "PrimaryKey"_("l_orderkey"_, "l_linenumber"_))));
    checkForErrors(evalStorage("AddConstraint"_("NATION"_, "PrimaryKey"_("n_nationkey"_))));
    checkForErrors(evalStorage("AddConstraint"_("REGION"_, "PrimaryKey"_("r_regionkey"_))));

    // foreign key constraints
    checkForErrors(
        evalStorage("AddConstraint"_("SUPPLIER"_, "ForeignKey"_("NATION"_, "s_nationkey"_))));
    checkForErrors(
        evalStorage("AddConstraint"_("PARTSUPP"_, "ForeignKey"_("PART"_, "ps_partkey"_))));
    checkForErrors(
        evalStorage("AddConstraint"_("PARTSUPP"_, "ForeignKey"_("SUPPLIER"_, "ps_suppkey"_))));
    checkForErrors(
        evalStorage("AddConstraint"_("CUSTOMER"_, "ForeignKey"_("NATION"_, "c_nationkey"_))));
    checkForErrors(
        evalStorage("AddConstraint"_("ORDERS"_, "ForeignKey"_("CUSTOMER"_, "o_custkey"_))));
    checkForErrors(
        evalStorage("AddConstraint"_("LINEITEM"_, "ForeignKey"_("ORDERS"_, "l_orderkey"_))));
    checkForErrors(evalStorage(
        "AddConstraint"_("LINEITEM"_, "ForeignKey"_("PARTSUPP"_, "l_partkey"_, "l_suppkey"_))));
    checkForErrors(
        evalStorage("AddConstraint"_("NATION"_, "ForeignKey"_("REGION"_, "n_regionkey"_))));
  }
}

static auto& tpchQueryNames() {
  static std::map<int, std::string> names;
  if(names.empty()) {
    names.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q1), "TPC-H_Q1");
    names.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q3), "TPC-H_Q3");
    names.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q6), "TPC-H_Q6");
    names.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q9), "TPC-H_Q9");
    names.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q18), "TPC-H_Q18");
    names.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q3_FIRST_JOIN_ONLY),
                      "TPC-H_Q3-FIRST-JOIN-ONLY");
    names.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q3_NOTOP),
                      "TPC-H_Q3-NO-TOP");
    names.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q9_SPLIT_TWO_KEYS),
                      "TPC-H_Q9-SPLIT-TWO-KEYS");
    names.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q18_SUBQUERY),
                      "TPC-H_Q18-SUBQUERY");
    names.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q18_NOTOP),
                      "TPC-H_Q18-NO-TOP");
  }
  return names;
}
static auto& etlQueriesNoTransformNames() {
  static std::map<int, std::string> names;
  if(names.empty()) {
    for (int sf : {1, 10, 100, 1000, 10000, 20000}) {
      for (int percentage : {20, 40, 60, 80, 100}){
        names.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::LINE_VIEW_03) , "LINE_03_NT_SF_" + std::to_string(sf) + "PC" + std::to_string(percentage));
        names.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::LINE_VIEW_04), "LINE_04_NT_SF_" + std::to_string(sf) + "PC" + std::to_string(percentage));
        names.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::BUTTERFLY_1_VIEW_12), "BUTT_12_NT_SF_" + std::to_string(sf) + "PC" + std::to_string(percentage));
        names.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::BUTTERFLY_1_VIEW_13), "BUTT_13_NT_SF_" + std::to_string(sf) + "PC" + std::to_string(percentage));
      }
    }
  }
  return names;
}

static auto& etlQueriesTransformNames() {
  static std::map<int, std::string> names;
  if(names.empty()) {
    names.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(ETL_TRANSFORMATIONS::LINE_VIEW_TRANSFORM) , "LINE_TRANSFORMATION_SETUP");
    names.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(ETL_TRANSFORMATIONS::BUTTERFLY_TRANSFORM) , "BUTT_TRANSFORMATION_SETUP");
        for (int sf : {1, 10, 100, 1000, 10000, 20000}) {
      for (int percentage : {20, 40, 60, 80, 100}){
        names.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::LINE_VIEW_03) , "LINE_03_TR_SF_" + std::to_string(sf) + "PC" + std::to_string(percentage));
        names.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::LINE_VIEW_04), "LINE_04_TR_SF_" + std::to_string(sf) + "PC" + std::to_string(percentage));
        names.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::BUTTERFLY_1_VIEW_12), "BUTT_12_TR_SF_" + std::to_string(sf) + "PC" + std::to_string(percentage));
        names.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::BUTTERFLY_1_VIEW_13), "BUTT_13_TR_SF_" + std::to_string(sf) + "PC" + std::to_string(percentage));
      }
    }
  }
  return names;
}

auto createView02(int sf, int percentage) {
  int keyFilter = sf * 2 * percentage;
  return boss::ComplexExpression("Group"_(
        "Select"_(
          "Group"_(
            "Select"_(
              "Project"_(
                "Project"_(
                  "LINEITEM"_, 
                  "As"_("l_newcurrencyextendedprice"_, "Times"_("l_extendedprice"_, 1.1), 
                        "l_tax"_,"l_tax"_, "l_discount"_, "l_discount"_, "l_returnflag"_, "l_returnflag"_,
                        "l_linestatus"_, "l_linestatus"_, "l_partkey"_, "l_partkey"_)),
                "As"_("profit"_, "Times"_("l_newcurrencyextendedprice"_, "Minus"_(1.0, "Plus"_("l_tax"_, "l_discount"_))),
                      "l_returnflag"_, "l_returnflag"_, "l_linestatus"_, "l_linestatus"_, "l_partkey"_, "l_partkey"_,
                      "l_newcurrencyextendedprice"_, "l_newcurrencyextendedprice"_)),
              "Where"_("Equal"_("l_returnflag"_, 1))),
            "By"_("l_partkey"_, "l_linestatus"_),
            "As"_("sum_extendedprice"_, "Sum"_("l_newcurrencyextendedprice"_), "sum_profit"_, "Sum"_("profit"_))), 
          "Where"_("And"_("Equal"_("l_linestatus"_, 1), "Greater"_(keyFilter, "l_partkey"_)))), 
        "By"_("l_partkey"_),
        "As"_("sum_sum_extended_price"_, "Sum"_("sum_extendedprice"_), "sum__sum_profit"_, "Sum"_("sum_profit"_))));
}

auto createView03(int sf, int percentage) {
  int keyFilter = sf * 2 * percentage;
  return boss::ComplexExpression("Group"_(
        "Select"_(
          "Group"_(
            "Select"_(
              "Project"_(
                "Project"_(
                  "LINEITEM"_, 
                  "As"_("l_newcurrencyextendedprice"_, "Times"_("l_extendedprice"_, 1.1), 
                        "l_tax"_,"l_tax"_, "l_discount"_, "l_discount"_, "l_returnflag"_, "l_returnflag"_,
                        "l_linestatus"_, "l_linestatus"_, "l_partkey"_, "l_partkey"_)),
                "As"_("profit"_, "Times"_("l_newcurrencyextendedprice"_, "Minus"_(1.0, "Plus"_("l_tax"_, "l_discount"_))),
                      "l_returnflag"_, "l_returnflag"_, "l_linestatus"_, "l_linestatus"_, "l_partkey"_, "l_partkey"_,
                      "l_newcurrencyextendedprice"_, "l_newcurrencyextendedprice"_)),
              "Where"_("Equal"_("l_returnflag"_, 1))),
            "By"_("l_partkey"_, "l_linestatus"_),
            "As"_("sum_extendedprice"_, "Sum"_("l_newcurrencyextendedprice"_), "sum_profit"_, "Sum"_("profit"_))), 
          "Where"_("And"_("Equal"_("l_linestatus"_, 0), "Greater"_(keyFilter, "l_partkey"_)))), 
        "By"_("l_partkey"_),
        "As"_("sum_sum_extended_price"_, "Sum"_("sum_extendedprice"_), "sum__sum_profit"_, "Sum"_("sum_profit"_))));
}

auto createView12(int sf, int percentage) {
  int keyFilter = sf * percentage * 2;
  return boss::ComplexExpression(
      "Group"_(
        "Group"_(
          "Select"_(
            "Join"_(
              "Project"_(
                "PARTSUPP"_, 
                "As"_("ps_partkey"_, "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_, 
                      "ps_availqty"_,"ps_availqty"_, "ps_supplycost"_, "ps_supplycost"_, 
                      "ps_total_cost"_, "Times"_("ps_supplycost"_, "ps_availqty"_))),
              "Project"_(
                "SUPPLIER"_, 
                "As"_("s_suppkey"_, "s_suppkey"_, "s_nationkey"_, "s_nationkey"_, 
                      "s_acctbalcurrency"_,"Times"_("s_acctbal"_, 1.1))),
              "Where"_("Equal"_("ps_suppkey"_, "s_suppkey"_))),
            "Where"_("Greater"_(keyFilter, "ps_partkey"_))), 
          "By"_("ps_partkey"_, "s_nationkey"_),
          "As"_("max_supplycost"_, "Max"_("ps_supplycost"_), "min_supplycost"_, "Min"_("ps_supplycost"_))),
        "By"_("ps_partkey"_),
        "As"_("max_max_supplycost"_, "Max"_("max_supplycost"_), "min_min_supplycost"_, "Min"_("min_supplycost"_))));
}

auto createView13(int sf, int percentage) {
  int keyFilter = (int) (sf * percentage / 10);
  return boss::ComplexExpression(
    "Group"_(
      "Group"_(
        "Select"_(
          "Join"_(
            "Project"_(
              "PARTSUPP"_, 
              "As"_("ps_partkey"_, "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_, 
                    "ps_availqty"_,"ps_availqty"_, "ps_supplycost"_, "ps_supplycost"_,
                    "ps_total_cost"_, "Times"_("ps_supplycost"_, "ps_availqty"_))),
            "Project"_(
              "SUPPLIER"_, 
              "As"_("s_suppkey"_, "s_suppkey"_, "s_nationkey"_, "s_nationkey"_, 
                    "s_acctbalcurrency"_,"Times"_("s_acctbal"_, 1.1))),
            "Where"_("Equal"_("ps_suppkey"_, "s_suppkey"_))), 
          "Where"_("Greater"_(keyFilter, "s_suppkey"_))), 
        "By"_("s_suppkey"_, "s_nationkey"_),
        "As"_("sum_total_cost"_, "Sum"_("ps_total_cost"_))),
      "By"_("s_suppkey"_),
      "As"_("sum_sum_total_cost"_, "Sum"_("sum_total_cost"_))));
}

auto createLineTransformation() {
  return boss::ComplexExpression("AddTransformation"_(
          "Group"_(
            "Select"_(
              "Project"_(
                "Project"_(
                  "LINEITEM"_, 
                  "As"_("l_newcurrencyextendedprice"_, "Times"_("l_extendedprice"_, 1.1), 
                        "l_tax"_,"l_tax"_, "l_discount"_, "l_discount"_, "l_returnflag"_, "l_returnflag"_,
                        "l_linestatus"_, "l_linestatus"_, "l_partkey"_, "l_partkey"_)),
                "As"_("profit"_, "Times"_("l_newcurrencyextendedprice"_, "Minus"_(1.0, "Plus"_("l_tax"_, "l_discount"_))),
                      "l_returnflag"_, "l_returnflag"_, "l_linestatus"_, "l_linestatus"_, "l_partkey"_, "l_partkey"_,
                      "l_newcurrencyextendedprice"_, "l_newcurrencyextendedprice"_)),
              "Where"_("Equal"_("l_returnflag"_, 1))),
            "By"_("l_partkey"_, "l_linestatus"_),
            "As"_("sum_extendedprice"_, "Sum"_("l_newcurrencyextendedprice"_), "sum_profit"_, "Sum"_("profit"_)))
  ));
}
auto createLineTransformationNoTransform() {
  return boss::ComplexExpression(
          "Group"_(
            "Select"_(
              "Project"_(
                "Project"_(
                  "LINEITEM"_, 
                  "As"_("l_newcurrencyextendedprice"_, "Times"_("l_extendedprice"_, 1.1), 
                        "l_tax"_,"l_tax"_, "l_discount"_, "l_discount"_, "l_returnflag"_, "l_returnflag"_,
                        "l_linestatus"_, "l_linestatus"_, "l_partkey"_, "l_partkey"_)),
                "As"_("profit"_, "Times"_("l_newcurrencyextendedprice"_, "Minus"_(1.0, "Plus"_("l_tax"_, "l_discount"_))),
                      "l_returnflag"_, "l_returnflag"_, "l_linestatus"_, "l_linestatus"_, "l_partkey"_, "l_partkey"_,
                      "l_newcurrencyextendedprice"_, "l_newcurrencyextendedprice"_)),
              "Where"_("Equal"_("l_returnflag"_, 1))),
            "By"_("l_partkey"_, "l_linestatus"_),
            "As"_("sum_extendedprice"_, "Sum"_("l_newcurrencyextendedprice"_), "sum_profit"_, "Sum"_("profit"_))
  ));
}

auto createButterflyTransformationNoTransform() {
  return boss::ComplexExpression("Join"_(
            "Project"_(
              "PARTSUPP"_, 
              "As"_("ps_partkey"_, "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_, 
                    "ps_availqty"_,"ps_availqty"_, "ps_supplycost"_, "ps_supplycost"_,
                    "ps_total_cost"_, "Times"_("ps_supplycost"_, "ps_availqty"_))),
            "Project"_(
              "SUPPLIER"_, 
              "As"_("s_suppkey"_, "s_suppkey"_, "s_nationkey"_, "s_nationkey"_, 
                    "s_acctbalcurrency"_,"Times"_("s_acctbal"_, 1.1))),
            "Where"_("Equal"_("ps_suppkey"_, "s_suppkey"_))));
}
auto createButterflyTransformation() {
  return boss::ComplexExpression("AddTransformation"_("Join"_(
            "Project"_(
              "PARTSUPP"_, 
              "As"_("ps_partkey"_, "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_, 
                    "ps_availqty"_,"ps_availqty"_, "ps_supplycost"_, "ps_supplycost"_,
                    "ps_total_cost"_, "Times"_("ps_supplycost"_, "ps_availqty"_))),
            "Project"_(
              "SUPPLIER"_, 
              "As"_("s_suppkey"_, "s_suppkey"_, "s_nationkey"_, "s_nationkey"_, 
                    "s_acctbalcurrency"_,"Times"_("s_acctbal"_, 1.1))),
            "Where"_("Equal"_("ps_suppkey"_, "s_suppkey"_)))));
}

auto createView02Transformation(int sf, int percentage) {
  int keyFilter = sf * 2 * percentage;
  return boss::ComplexExpression("ApplyTransformation"_(
    "Group"_(
      "Select"_(
        "Transformation"_, 
        "Where"_("And"_("Equal"_("l_linestatus"_, 1), "Greater"_(keyFilter, "l_partkey"_)))), 
      "By"_("l_partkey"_),
      "As"_("sum_sum_extended_price"_, "Sum"_("sum_extendedprice"_), "sum_sum_profit"_, "Sum"_("sum_profit"_))),
      0));
}

auto createView03Transformation(int sf, int percentage) {
  int keyFilter = sf * 2 * percentage;
  return boss::ComplexExpression("ApplyTransformation"_(
    "Group"_(
      "Select"_(
        "Transformation"_, 
        "Where"_("And"_("Equal"_("l_linestatus"_, 0), "Greater"_(keyFilter, "l_partkey"_)))), 
      "By"_("l_partkey"_),
      "As"_("sum_sum_extended_price"_, "Sum"_("sum_extendedprice"_), "sum_sum_profit"_, "Sum"_("sum_profit"_))), 
      0));
}

auto createView12Transformation(int sf, int percentage) {
  int keyFilter = sf * percentage * 2;
  return boss::ComplexExpression("ApplyTransformation"_(
      "Group"_(
        "Group"_(
          "Select"_(
            "Transformation"_,
            "Where"_("Greater"_(keyFilter, "ps_partkey"_))), 
          "By"_("ps_partkey"_, "s_nationkey"_),
          "As"_("max_supplycost"_, "Max"_("ps_supplycost"_), "min_supplycost"_, "Min"_("ps_supplycost"_))),
        "By"_("ps_partkey"_),
        "As"_("max_max_supplycost"_, "Max"_("max_supplycost"_), "min_min_supplycost"_, "Min"_("min_supplycost"_))),
        0));
}
auto createView13Transformation(int sf, int percentage) {
  int keyFilter = (int) (sf * percentage / 10);
  return boss::ComplexExpression("ApplyTransformation"_(
    "Group"_(
      "Group"_(
        "Select"_(
          "Transformation"_,
          "Where"_("Greater"_(keyFilter, "ps_partkey"_))), 
        "By"_("s_suppkey"_, "s_nationkey"_),
        "As"_("sum_total_cost"_, "Sum"_("ps_total_cost"_))),
      "By"_("s_suppkey"_),
      "As"_("sum_sum_total_cost"_, "Sum"_("sum_total_cost"_))),
      0));
}
static auto& etlTransformationNames() {
  static std::map<int, std::string> names;
  if(names.empty()) {
        names.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(TRANSFORMATION::LINE), "LineTransform");
        names.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(TRANSFORMATION::BUTTERFLY), "ButterflyTransform");
  }
  return names;
}

auto& etlTransformationsQueries() {
    // Queries are Expressions and therefore cannot be just a table i.e. a Symbol
  static std::map<int, boss::Expression> queries;
  if(queries.empty()) {
        queries.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(TRANSFORMATION::LINE), createLineTransformationNoTransform());
        queries.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(TRANSFORMATION::BUTTERFLY), createButterflyTransformationNoTransform());
  }
  return queries;
}
auto& etlQueriesNoTransform() {
    // Queries are Expressions and therefore cannot be just a table i.e. a Symbol
  static std::map<int, boss::Expression> queries;
  if(queries.empty()) {
    for (int sf : {1, 10, 100, 1000, 10000, 20000}) {
      for (int percentage : {20, 40, 60, 80, 100}){
        queries.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::LINE_VIEW_03), createView02(sf, percentage));
        queries.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::LINE_VIEW_04), createView03(sf, percentage));
        queries.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::BUTTERFLY_1_VIEW_12), createView12(sf, percentage));
        queries.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::BUTTERFLY_1_VIEW_13), createView13(sf, percentage));
      }
    }
  }
  return queries;
}
auto& etlQueriesTransform() {
    // Queries are Expressions and therefore cannot be just a table i.e. a Symbol
  static std::map<int, boss::Expression> queries;
  if(queries.empty()) {
    queries.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(ETL_TRANSFORMATIONS::LINE_VIEW_TRANSFORM), createLineTransformation());
    queries.try_emplace(static_cast<int>(DATASETS::TPCH) + static_cast<int>(ETL_TRANSFORMATIONS::BUTTERFLY_TRANSFORM) , createButterflyTransformation());

    for (int sf : {1, 10, 100, 1000, 10000, 20000}) {
      for (int percentage : {20, 40, 60, 80, 100}){
        queries.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::LINE_VIEW_03), createView02Transformation(sf, percentage));
        queries.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::LINE_VIEW_04), createView03Transformation(sf, percentage));
        queries.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::BUTTERFLY_1_VIEW_12), createView12Transformation(sf, percentage));
        queries.try_emplace(static_cast<int>(DATASETS::TPCH) + sf + percentage + static_cast<int>(ETL_QUERIES::BUTTERFLY_1_VIEW_13), createView13Transformation(sf, percentage));
      }
    }
  }
  return queries;
}

auto& tpchQueries() {
  // Queries are Expressions and therefore cannot be just a table i.e. a Symbol
  static std::map<int, boss::Expression> queries;
  if(queries.empty()) {
    queries.try_emplace(
        static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q1),
        "Order"_(
            "Project"_(
                "Group"_(
                    "Project"_(
                        "Project"_(
                            "Project"_(
                                "Select"_(
                                    "Project"_("LINEITEM"_,
                                               "As"_("l_quantity"_, "l_quantity"_, "l_discount"_,
                                                     "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                                     "l_extendedprice"_, "l_extendedprice"_,
                                                     "l_returnflag"_, "l_returnflag"_,
                                                     "l_linestatus"_, "l_linestatus"_, "l_tax"_,
                                                     "l_tax"_)),
                                    "Where"_(
                                        "Greater"_("DateObject"_("1998-08-31"), "l_shipdate"_))),
                                "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                                      "l_linestatus"_, "l_quantity"_, "l_quantity"_,
                                      "l_extendedprice"_, "l_extendedprice"_, "l_discount"_,
                                      "l_discount"_, "calc1"_, "Minus"_(1.0, "l_discount"_),
                                      "calc2"_, "Plus"_("l_tax"_, 1.0))),
                            "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_,
                                  "l_linestatus"_, "l_quantity"_, "l_quantity"_, "l_extendedprice"_,
                                  "l_extendedprice"_, "l_discount"_, "l_discount"_, "disc_price"_,
                                  "Times"_("l_extendedprice"_, "calc1"_), "calc2"_, "calc2"_)),
                        "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_, "l_linestatus"_,
                              "l_quantity"_, "l_quantity"_, "l_extendedprice"_, "l_extendedprice"_,
                              "l_discount"_, "l_discount"_, "disc_price"_, "disc_price"_, "calc"_,
                              "Times"_("disc_price"_, "calc2"_))),
                    "By"_("l_returnflag"_, "l_linestatus"_),
                    // disable one aggregate to limit it to 5 aggregate max for now
                    // (to reduce compilation time for the hazard-adaptive engine)
                    "As"_("sum_qty"_, "Sum"_("l_quantity"_), "sum_base_price"_,
                          "Sum"_("l_extendedprice"_), "sum_disc_price"_, "Sum"_("disc_price"_),
                          "sum_charges"_, "Sum"_("calc"_), /*"sum_disc"_, "Sum"_("l_discount"_),*/
                          "count_order"_, "Count"_("l_quantity"_))),
                "As"_("l_returnflag"_, "l_returnflag"_, "l_linestatus"_, "l_linestatus"_,
                      "sum_qty"_, "sum_qty"_, "sum_base_price"_, "sum_base_price"_,
                      "sum_disc_price"_, "sum_disc_price"_, "sum_charges"_, "sum_charges"_,
                      "avg_qty"_, "Divide"_("sum_qty"_, "count_order"_), "avg_price"_,
                      "Divide"_("sum_base_price"_, "count_order"_), /*"avg_disc"_,
                      "Divide"_("sum_disc"_, "count_order"_),*/
                      "count_order"_, "count_order"_)),
            "By"_("l_returnflag"_, "l_linestatus"_)));

    queries.try_emplace(
        static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q3_FIRST_JOIN_ONLY),
        "Order"_(
            "Project"_(
                "Join"_(
                    "Project"_(
                        "Select"_("Project"_("CUSTOMER"_, "As"_("c_custkey"_, "c_custkey"_,
                                                                "c_mktsegment"_, "c_mktsegment"_)),
                                  "Where"_("StringContainsQ"_("c_mktsegment"_, "BUILDING"))),
                        "As"_("c_custkey"_, "c_custkey"_, "c_mktsegment"_, "c_mktsegment"_)),
                    "Select"_(
                        "Project"_("ORDERS"_, "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                                                    "o_orderdate"_, "o_custkey"_, "o_custkey"_,
                                                    "o_shippriority"_, "o_shippriority"_)),
                        "Where"_("Greater"_("DateObject"_("1995-03-15"), "o_orderdate"_))),
                    "Where"_("Equal"_("c_custkey"_, "o_custkey"_))),
                "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_, "o_orderdate"_, "o_custkey"_,
                      "o_custkey"_, "o_shippriority"_, "o_shippriority"_)),
            "By"_("o_custkey"_, "o_orderkey"_, "o_orderdate"_, "o_shippriority"_)));

    queries.try_emplace(
        static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q3),
        "Top"_(
            "Group"_(
                "Project"_(
                    "Join"_(
                        "Project"_(
                            "Join"_(
                                "Project"_(
                                    "Select"_("Project"_("CUSTOMER"_,
                                                         "As"_("c_custkey"_, "c_custkey"_,
                                                               "c_mktsegment"_, "c_mktsegment"_)),
                                              "Where"_(
                                                  "StringContainsQ"_("c_mktsegment"_, "BUILDING"))),
                                    "As"_("c_custkey"_, "c_custkey"_, "c_mktsegment"_,
                                          "c_mktsegment"_)),
                                "Select"_(
                                    "Project"_("ORDERS"_,
                                               "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                                                     "o_orderdate"_, "o_custkey"_, "o_custkey"_,
                                                     "o_shippriority"_, "o_shippriority"_)),
                                    "Where"_(
                                        "Greater"_("DateObject"_("1995-03-15"), "o_orderdate"_))),
                                "Where"_("Equal"_("c_custkey"_, "o_custkey"_))),
                            "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_, "o_orderdate"_,
                                  "o_custkey"_, "o_custkey"_, "o_shippriority"_,
                                  "o_shippriority"_)),
                        "Project"_(
                            "Select"_(
                                "Project"_("LINEITEM"_,
                                           "As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_,
                                                 "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                                 "l_extendedprice"_, "l_extendedprice"_)),
                                "Where"_("Greater"_("l_shipdate"_, "DateObject"_("1995-03-15")))),
                            "As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_, "l_discount"_,
                                  "l_extendedprice"_, "l_extendedprice"_)),
                        "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
                    "As"_("expr1009"_, "Times"_("l_extendedprice"_, "Minus"_(1.0, "l_discount"_)),
                          "l_extendedprice"_, "l_extendedprice"_, "l_orderkey"_, "l_orderkey"_,
                          "o_orderdate"_, "o_orderdate"_, "o_shippriority"_, "o_shippriority"_)),
                "By"_("l_orderkey"_),
                "As"_("revenue"_, "Sum"_("expr1009"_), "o_orderdate"_, "Min"_("o_orderdate"_),
                      "o_shippriority"_, "Min"_("o_shippriority"_))),
            "By"_("revenue"_, "desc"_, "o_orderdate"_), 10));

    queries.try_emplace(
        static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q3_NOTOP),
        "Group"_(
            "Project"_(
                "Join"_(
                    "Project"_(
                        "Join"_(
                            "Project"_(
                                "Select"_(
                                    "Project"_("CUSTOMER"_,
                                               "As"_("c_custkey"_, "c_custkey"_, "c_mktsegment"_,
                                                     "c_mktsegment"_)),
                                    "Where"_("StringContainsQ"_("c_mktsegment"_, "BUILDING"))),
                                "As"_("c_custkey"_, "c_custkey"_, "c_mktsegment"_,
                                      "c_mktsegment"_)),
                            "Select"_("Project"_("ORDERS"_,
                                                 "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                                                       "o_orderdate"_, "o_custkey"_, "o_custkey"_,
                                                       "o_shippriority"_, "o_shippriority"_)),
                                      "Where"_(
                                          "Greater"_("DateObject"_("1995-03-15"), "o_orderdate"_))),
                            "Where"_("Equal"_("c_custkey"_, "o_custkey"_))),
                        "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_, "o_orderdate"_,
                              "o_custkey"_, "o_custkey"_, "o_shippriority"_, "o_shippriority"_)),
                    "Project"_(
                        "Select"_("Project"_("LINEITEM"_,
                                             "As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_,
                                                   "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                                   "l_extendedprice"_, "l_extendedprice"_)),
                                  "Where"_("Greater"_("l_shipdate"_, "DateObject"_("1995-03-15")))),
                        "As"_("l_orderkey"_, "l_orderkey"_, "l_discount"_, "l_discount"_,
                              "l_extendedprice"_, "l_extendedprice"_)),
                    "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
                "As"_("expr1009"_, "Times"_("l_extendedprice"_, "Minus"_(1.0, "l_discount"_)),
                      "l_extendedprice"_, "l_extendedprice"_, "l_orderkey"_, "l_orderkey"_,
                      "o_orderdate"_, "o_orderdate"_, "o_shippriority"_, "o_shippriority"_)),
            "By"_("l_orderkey"_),
            "As"_("revenue"_, "Sum"_("expr1009"_), "o_orderdate"_, "Min"_("o_orderdate"_),
                  "o_shippriority"_, "Min"_("o_shippriority"_))));

    queries.try_emplace(
        static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q6),
        "Group"_(
            "Project"_(
                "Select"_("Project"_("LINEITEM"_, "As"_("l_quantity"_, "l_quantity"_, "l_discount"_,
                                                        "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                                        "l_extendedprice"_, "l_extendedprice"_)),
                          "Where"_("And"_("Greater"_(24, "l_quantity"_),      // NOLINT
                                          "Greater"_("l_discount"_, 0.0499),  // NOLINT
                                          "Greater"_(0.07001, "l_discount"_), // NOLINT
                                          "Greater"_("DateObject"_("1995-01-01"), "l_shipdate"_),
                                          "Greater"_("l_shipdate"_, "DateObject"_("1993-12-31"))))),
                "As"_("revenue"_, "Times"_("l_extendedprice"_, "l_discount"_))),
            "As"_("revenue"_, "Sum"_("revenue"_))));

    queries.try_emplace(
        static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q9),
        "Order"_(
            "Group"_(
                "Project"_(
                    "Join"_(
                        "Project"_("ORDERS"_, "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                                                    "o_orderdate"_)),
                        "Project"_(
                            "Join"_(
                                "Project"_(
                                    "Join"_(
                                        "Project"_(
                                            "Select"_(
                                                "Project"_("PART"_,
                                                           "As"_("p_partkey"_, "p_partkey"_,
                                                                 "p_retailprice"_,
                                                                 "p_retailprice"_)),
                                                "Where"_("And"_("Greater"_("p_retailprice"_,
                                                                           1006.05), // NOLINT
                                                                "Greater"_(1080.1,   // NOLINT
                                                                           "p_retailprice"_)))),
                                            "As"_("p_partkey"_, "p_partkey"_, "p_retailprice"_,
                                                  "p_retailprice"_)),
                                        "Project"_(
                                            "Join"_(
                                                "Project"_(
                                                    "Join"_(
                                                        "Project"_("NATION"_,
                                                                   "As"_("n_name"_, "n_name"_,
                                                                         "n_nationkey"_,
                                                                         "n_nationkey"_)),
                                                        "Project"_("SUPPLIER"_,
                                                                   "As"_("s_suppkey"_, "s_suppkey"_,
                                                                         "s_nationkey"_,
                                                                         "s_nationkey"_)),
                                                        "Where"_("Equal"_("n_nationkey"_,
                                                                          "s_nationkey"_))),
                                                    "As"_("n_name"_, "n_name"_, "s_suppkey"_,
                                                          "s_suppkey"_)),
                                                "Project"_("PARTSUPP"_,
                                                           "As"_("ps_partkey"_, "ps_partkey"_,
                                                                 "ps_suppkey"_, "ps_suppkey"_,
                                                                 "ps_supplycost"_,
                                                                 "ps_supplycost"_)),
                                                "Where"_("Equal"_("s_suppkey"_, "ps_suppkey"_))),
                                            "As"_("n_name"_, "n_name"_, "ps_partkey"_,
                                                  "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_,
                                                  "ps_supplycost"_, "ps_supplycost"_)),
                                        "Where"_("Equal"_("p_partkey"_, "ps_partkey"_))),
                                    "As"_("n_name"_, "n_name"_, "ps_partkey"_, "ps_partkey"_,
                                          "ps_suppkey"_, "ps_suppkey"_, "ps_supplycost"_,
                                          "ps_supplycost"_)),
                                "Project"_("LINEITEM"_,
                                           "As"_("l_partkey"_, "l_partkey"_, "l_suppkey"_,
                                                 "l_suppkey"_, "l_orderkey"_, "l_orderkey"_,
                                                 "l_extendedprice"_, "l_extendedprice"_,
                                                 "l_discount"_, "l_discount"_, "l_quantity"_,
                                                 "l_quantity"_)),
                                "Where"_("Equal"_("List"_("ps_partkey"_, "ps_suppkey"_),
                                                  "List"_("l_partkey"_, "l_suppkey"_)))),
                            "As"_("n_name"_, "n_name"_, "ps_supplycost"_, "ps_supplycost"_,
                                  "l_orderkey"_, "l_orderkey"_, "l_extendedprice"_,
                                  "l_extendedprice"_, "l_discount"_, "l_discount"_, "l_quantity"_,
                                  "l_quantity"_)),
                        "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
                    "As"_("nation"_, "n_name"_, "o_year"_, "Year"_("o_orderdate"_), "amount"_,
                          "Minus"_("Times"_("l_extendedprice"_, "Minus"_(1.0, "l_discount"_)),
                                   "Times"_("ps_supplycost"_, "l_quantity"_)))),
                "By"_("nation"_, "o_year"_), "As"_("amount"_, "Sum"_("amount"_))),
            "By"_("nation"_, "o_year"_, "desc"_)));

    queries.try_emplace(
        static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q9_SPLIT_TWO_KEYS),
        "Order"_(
            "Group"_(
                "Project"_(
                    "Join"_(
                        "Project"_("ORDERS"_, "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_,
                                                    "o_orderdate"_)),
                        "Project"_(
                            "Select"_(
                                "Join"_(
                                    "Project"_(
                                        "Join"_(
                                            "Project"_(
                                                "Select"_(
                                                    "Project"_("PART"_,
                                                               "As"_("p_partkey"_, "p_partkey"_,
                                                                     "p_retailprice"_,
                                                                     "p_retailprice"_)),
                                                    "Where"_("And"_("Greater"_("p_retailprice"_,
                                                                               1006.05), // NOLINT
                                                                    "Greater"_(1080.1,   // NOLINT
                                                                               "p_retailprice"_)))),
                                                "As"_("p_partkey"_, "p_partkey"_, "p_retailprice"_,
                                                      "p_retailprice"_)),
                                            "Project"_(
                                                "Join"_(
                                                    "Project"_(
                                                        "Join"_(
                                                            "Project"_("NATION"_,
                                                                       "As"_("n_name"_, "n_name"_,
                                                                             "n_nationkey"_,
                                                                             "n_nationkey"_)),
                                                            "Project"_("SUPPLIER"_,
                                                                       "As"_("s_suppkey"_,
                                                                             "s_suppkey"_,
                                                                             "s_nationkey"_,
                                                                             "s_nationkey"_)),
                                                            "Where"_("Equal"_("n_nationkey"_,
                                                                              "s_nationkey"_))),
                                                        "As"_("n_name"_, "n_name"_, "s_suppkey"_,
                                                              "s_suppkey"_)),
                                                    "Project"_("PARTSUPP"_,
                                                               "As"_("ps_partkey"_, "ps_partkey"_,
                                                                     "ps_suppkey"_, "ps_suppkey"_,
                                                                     "ps_supplycost"_,
                                                                     "ps_supplycost"_)),
                                                    "Where"_(
                                                        "Equal"_("s_suppkey"_, "ps_suppkey"_))),
                                                "As"_("n_name"_, "n_name"_, "ps_partkey"_,
                                                      "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_,
                                                      "ps_supplycost"_, "ps_supplycost"_)),
                                            "Where"_("Equal"_("p_partkey"_, "ps_partkey"_))),
                                        "As"_("n_name"_, "n_name"_, "ps_partkey"_, "ps_partkey"_,
                                              "ps_suppkey"_, "ps_suppkey"_, "ps_supplycost"_,
                                              "ps_supplycost"_)),
                                    "Project"_("LINEITEM"_,
                                               "As"_("l_partkey"_, "l_partkey"_, "l_suppkey"_,
                                                     "l_suppkey"_, "l_orderkey"_, "l_orderkey"_,
                                                     "l_extendedprice"_, "l_extendedprice"_,
                                                     "l_discount"_, "l_discount"_, "l_quantity"_,
                                                     "l_quantity"_)),
                                    "Where"_("Equal"_("ps_partkey"_, "l_partkey"_))),
                                "Where"_("Equal"_("ps_suppkey"_, "l_suppkey"_))),
                            "As"_("n_name"_, "n_name"_, "ps_supplycost"_, "ps_supplycost"_,
                                  "l_orderkey"_, "l_orderkey"_, "l_extendedprice"_,
                                  "l_extendedprice"_, "l_discount"_, "l_discount"_, "l_quantity"_,
                                  "l_quantity"_)),
                        "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
                    "As"_("nation"_, "n_name"_, "o_year"_, "Year"_("o_orderdate"_), "amount"_,
                          "Minus"_("Times"_("l_extendedprice"_, "Minus"_(1.0, "l_discount"_)),
                                   "Times"_("ps_supplycost"_, "l_quantity"_)))),
                "By"_("nation"_, "o_year"_), "As"_("amount"_, "Sum"_("amount"_))),
            "By"_("nation"_, "o_year"_, "desc"_)));

    queries.try_emplace(
        static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q18),
        "Top"_(
            "Group"_(
                "Project"_(
                    "Join"_(
                        "Project"_(
                            "Join"_("Project"_("CUSTOMER"_, "As"_("c_custkey"_, "c_custkey"_)),
                                    "Project"_("ORDERS"_,
                                               "As"_("o_orderkey"_, "o_orderkey"_, "o_custkey"_,
                                                     "o_custkey"_, "o_orderdate"_, "o_orderdate"_,
                                                     "o_totalprice"_, "o_totalprice"_)),
                                    "Where"_("Equal"_("c_custkey"_, "o_custkey"_))),
                            "As"_("o_orderkey"_, "o_orderkey"_, "o_custkey"_, "o_custkey"_,
                                  "o_orderdate"_, "o_orderdate"_, "o_totalprice"_,
                                  "o_totalprice"_)),
                        "Select"_(
                            "Group"_("Project"_("LINEITEM"_, "As"_("l_orderkey"_, "l_orderkey"_,
                                                                   "l_quantity"_, "l_quantity"_)),
                                     "By"_("l_orderkey"_),
                                     "As"_("sum_l_quantity"_, "Sum"_("l_quantity"_))),
                            "Where"_("Greater"_("sum_l_quantity"_, 300))), // NOLINT
                        "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
                    "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_, "o_orderdate"_,
                          "o_totalprice"_, "o_totalprice"_, "o_custkey"_, "o_custkey"_,
                          "sum_l_quantity"_, "sum_l_quantity"_)),
                "By"_("o_orderkey"_),
                "As"_("sum_l_quantity"_, "Sum"_("sum_l_quantity"_), "o_custkey"_,
                      "Min"_("o_custkey"_), "o_orderdate"_, "Min"_("o_orderdate"_), "o_totalprice"_,
                      "Min"_("o_totalprice"_))),
            "By"_("o_totalprice"_, "desc"_, "o_orderdate"_), 100));

    queries.try_emplace(
        static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q18_SUBQUERY),
        "Group"_("Project"_("LINEITEM"_,
                            "As"_("l_orderkey"_, "l_orderkey"_, "l_quantity"_, "l_quantity"_)),
                 "By"_("l_orderkey"_), "As"_("sum_l_quantity"_, "Sum"_("l_quantity"_)))); // NOLINT

    queries.try_emplace(
        static_cast<int>(DATASETS::TPCH) + static_cast<int>(TPCH_Q18_NOTOP),
        "Group"_(
            "Project"_(
                "Join"_(
                    "Project"_(
                        "Join"_("Project"_("CUSTOMER"_, "As"_("c_custkey"_, "c_custkey"_)),
                                "Project"_("ORDERS"_,
                                           "As"_("o_orderkey"_, "o_orderkey"_, "o_custkey"_,
                                                 "o_custkey"_, "o_orderdate"_, "o_orderdate"_,
                                                 "o_totalprice"_, "o_totalprice"_)),
                                "Where"_("Equal"_("c_custkey"_, "o_custkey"_))),
                        "As"_("o_orderkey"_, "o_orderkey"_, "o_custkey"_, "o_custkey"_,
                              "o_orderdate"_, "o_orderdate"_, "o_totalprice"_, "o_totalprice"_)),
                    "Select"_("Group"_("Project"_("LINEITEM"_, "As"_("l_orderkey"_, "l_orderkey"_,
                                                                     "l_quantity"_, "l_quantity"_)),
                                       "By"_("l_orderkey"_),
                                       "As"_("sum_l_quantity"_, "Sum"_("l_quantity"_))),
                              "Where"_("Greater"_("sum_l_quantity"_, 300))), // NOLINT
                    "Where"_("Equal"_("o_orderkey"_, "l_orderkey"_))),
                "As"_("o_orderkey"_, "o_orderkey"_, "o_orderdate"_, "o_orderdate"_, "o_totalprice"_,
                      "o_totalprice"_, "o_custkey"_, "o_custkey"_, "sum_l_quantity"_,
                      "sum_l_quantity"_)),
            "By"_("o_orderkey"_),
            "As"_("sum_l_quantity"_, "Sum"_("sum_l_quantity"_), "o_custkey"_, "Min"_("o_custkey"_),
                  "o_orderdate"_, "Min"_("o_orderdate"_), "o_totalprice"_,
                  "Min"_("o_totalprice"_))));
  }
  return queries;
}

void setTPCH_groupResultCardinality(int queryIdx, int dataSize) {
  int groupResultCardinality = 1;
  if(queryIdx == TPCH_Q1) {
    switch(dataSize) { // NOLINT
    case 1:
      groupResultCardinality = 4;
      break;
    case 10:
      groupResultCardinality = 4;
      break;
    case 100:
      groupResultCardinality = 4;
      break;
    case 1000:
      groupResultCardinality = 4;
      break;
    case 10000:
      groupResultCardinality = 4;
      break;
    case 100000:
      groupResultCardinality = 4;
      break;
    }
  } else if(queryIdx == TPCH_Q3) {
    switch(dataSize) { // NOLINT
    case 1:
      groupResultCardinality = 8;
      break;
    case 10:
      groupResultCardinality = 138;
      break;
    case 100:
      groupResultCardinality = 1216;
      break;
    case 1000:
      groupResultCardinality = 11620;
      break;
    case 10000:
      groupResultCardinality = 114003;
      break;
    case 100000:
      groupResultCardinality = 1131041;
      break;
    }
  } else if(queryIdx == TPCH_Q9) {
    switch(dataSize) { // NOLINT
    case 1:
      groupResultCardinality = 72;
      break;
    case 10:
      groupResultCardinality = 191;
      break;
    case 100:
      groupResultCardinality = 200;
      break;
    case 1000:
      groupResultCardinality = 200;
      break;
    case 10000:
      groupResultCardinality = 200;
      break;
    case 100000:
      groupResultCardinality = 200;
      break;
    }
  } else if(queryIdx == TPCH_Q18) {
    switch(dataSize) { // NOLINT
    case 1:
      groupResultCardinality = 1500; // max(0, 1500)
      break;
    case 10:
      groupResultCardinality = 15000; // max(2, 15000)
      break;
    case 100:
      groupResultCardinality = 150000; // max(5, 150000)
      break;
    case 1000:
      groupResultCardinality = 1500000; // max(57, 1500000)
      break;
    case 10000:
      groupResultCardinality = 15000000; // max(624, 15000000)
      break;
    case 100000:
      groupResultCardinality = 150000000; // max(6398, 150000000)
      break;
    }
  }
  setCardinalityEnvironmentVariable(groupResultCardinality);
}

void TPCH_Benchmark(benchmark::State& state, int queryIdx, int dataSize, int blockSize) {
  initStorageEngine_TPCH(dataSize, blockSize);
  setTPCH_groupResultCardinality(queryIdx, dataSize);

  // auto const& queryName = etlQueriesNoTransformNames().find(queryIdx)->second;
  // auto const& query = etlQueriesNoTransform().find(queryIdx)->second;
  auto const& queryName = etlTransformationNames().find(queryIdx)->second;
  auto const& query = etlTransformationsQueries().find(queryIdx)->second;
  // auto const& queryName = etlQueriesTransformNames().find(queryIdx)->second;
  // auto const& query = etlQueriesTransform().find(queryIdx)->second;

  runBenchmark(state, queryName, query);
}

void initStorageEngine_tpch_q6_clustering(int dataSize, uint32_t spreadInCluster) {
  auto evalStorage = getEvaluateStorageLambda();
  auto checkForErrors = getCheckForErrorsLambda();

  if(latestDataSet == "tpch_q6_clustering_sweep" && latestDataSize == dataSize) {
    checkForErrors(evalStorage("DropTable"_("LINEITEM_CLUSTERED"_)));
  } else {
    resetStorageEngine();
    checkForErrors(evalStorage(
        "CreateTable"_("LINEITEM"_, "l_orderkey"_, "l_partkey"_, "l_suppkey"_, "l_linenumber"_,
                       "l_quantity"_, "l_extendedprice"_, "l_discount"_, "l_tax"_, "l_returnflag"_,
                       "l_linestatus"_, "l_shipdate"_, "l_commitdate"_, "l_receiptdate"_,
                       "l_shipinstruct"_, "l_shipmode"_, "l_comment"_)));
    std::string path =
        tpch_filePath_prefix + "data/tpch_" + std::to_string(dataSize) + "MB/lineitem.tbl";
    checkForErrors(evalStorage("Load"_("LINEITEM"_, path)));
  }

  latestDataSet = "tpch_q6_clustering_sweep";
  latestDataSize = dataSize;

  auto originalTable = evalStorage(("LINEITEM"_));
  auto table = originalTable.clone(boss::expressions::CloneReason::EXPRESSION_SUBSTITUTION);

  std::string filepath =
      tpch_filePath_prefix + "data/tpch_" + std::to_string(dataSize) + "MB/lineitem.tbl";
  auto n = static_cast<int>(getNumberOfRowsInTable(filepath));
  const auto clusteringOrder = generateClusteringOrder(n, static_cast<int>(spreadInCluster));

  auto& originalColumns = std::get<ComplexExpression>(originalTable).getDynamicArguments();

  auto numColumns = std::get<ComplexExpression>(table).getDynamicArguments().size();
  auto columns = std::move(std::get<ComplexExpression>(table)).getDynamicArguments();

  for(auto i = 0; i < static_cast<int>(numColumns); ++i) {
    auto& originalColumn = originalColumns.at(i);
    auto& originalList = std::get<ComplexExpression>(originalColumn).getDynamicArguments().at(0);
    auto& originalSpan = std::get<ComplexExpression>(originalList).getSpanArguments().at(0);

    auto column = std::get<ComplexExpression>(std::move(columns.at(i)));
    auto [head, unused_, dynamics, spans] = std::move(column).decompose();
    auto list = std::get<ComplexExpression>(std::move(dynamics.at(0)));
    auto [listHead, listUnused_, listDynamics, listSpans] = std::move(list).decompose();

    listSpans.at(0) = std::visit(
        [&clusteringOrder, &originalSpan,
         n]<typename T>(boss::Span<T>&& typedSpan) -> SpanArgument {
          if constexpr(std::is_same_v<T, int64_t> || std::is_same_v<T, double_t> ||
                       std::is_same_v<T, std::string>) {
            return applyClusteringOrderToSpan(std::get<boss::Span<T>>(originalSpan),
                                              std::move(typedSpan), clusteringOrder, n);
          } else {
            throw std::runtime_error("unsupported column type in predicate");
          }
        },
        std::move(listSpans.at(0)));
    dynamics.at(0) =
        ComplexExpression(std::move(listHead), {}, std::move(listDynamics), std::move(listSpans));
    columns.at(i) = ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
  }

  checkForErrors(evalStorage("CreateTable"_("LINEITEM_CLUSTERED"_)));
  checkForErrors(evalStorage("LoadDataTable"_(
      "LINEITEM_CLUSTERED"_, ComplexExpression("Data"_, {}, std::move(columns), {}))));
}

void tpch_q6_clustering_sweep_Benchmark(benchmark::State& state, int dataSize) {
  uint32_t spreadInCluster = state.range(0);
  initStorageEngine_tpch_q6_clustering(dataSize, spreadInCluster);

  auto const& queryName = tpchQueryNames().find(TPCH_QUERIES::TPCH_Q6)->second;
  auto const& query = boss::Expression{"Group"_(
      "Project"_(
          "Select"_(
              "Project"_("LINEITEM_CLUSTERED"_, "As"_("l_quantity"_, "l_quantity"_, "l_discount"_,
                                                      "l_discount"_, "l_shipdate"_, "l_shipdate"_,
                                                      "l_extendedprice"_, "l_extendedprice"_)),
              "Where"_("And"_("Greater"_(24, "l_quantity"_),      // NOLINT
                              "Greater"_("l_discount"_, 0.0499),  // NOLINT
                              "Greater"_(0.07001, "l_discount"_), // NOLINT
                              "Greater"_("DateObject"_("1995-01-01"), "l_shipdate"_),
                              "Greater"_("l_shipdate"_, "DateObject"_("1993-12-31"))))),
          "As"_("revenue"_, "Times"_("l_extendedprice"_, "l_discount"_))),
      "As"_("revenue"_, "Sum"_("revenue"_)))};

  runBenchmark(state, queryName, query);
}