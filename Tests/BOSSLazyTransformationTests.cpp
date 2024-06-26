#include <BOSS.hpp>
#define CATCH_CONFIG_RUNNER
#include <ExpressionUtilities.hpp>
#include <array>
#include <catch2/catch.hpp>
#include <numeric>
#include <string_view>
#include <typeinfo>
#include <unordered_set>
#include <variant>
#include <vector>

#include "../Source/BOSSLazyTransformationEngine.hpp"
#include "../Source/Utilities.hpp"

using boss::Expression;
using std::string;
using std::literals::string_literals::operator""s;  // NOLINT(misc-unused-using-decls)
                                                    // clang-tidy bug
using boss::utilities::operator""_;                 // NOLINT(misc-unused-using-decls)
                                                    // clang-tidy bug
using Catch::Generators::random;
using Catch::Generators::take;
using Catch::Generators::values;
using std::vector;
using namespace Catch::Matchers;
using boss::engines::LazyTransformation::moveExctractedSelectExpressionToTransformation;
using boss::engines::LazyTransformation::removeUnusedTransformationColumns;
using boss::engines::LazyTransformation::replaceTransformSymbolsWithQuery;
using boss::engines::LazyTransformation::utilities::buildColumnDependencies;
using boss::engines::LazyTransformation::utilities::canMoveConditionThroughProjection;
using boss::engines::LazyTransformation::utilities::getAllDependentSymbols;
using boss::engines::LazyTransformation::utilities::getUsedSymbolsFromExpressions;
using boss::engines::LazyTransformation::utilities::getUsedTransformationColumns;
using boss::engines::LazyTransformation::utilities::isCardinalityReducingOperator;
using boss::engines::LazyTransformation::utilities::isConditionMoveable;
using boss::engines::LazyTransformation::utilities::isInTransformationColumns;
using boss::engines::LazyTransformation::utilities::isOperationReversible;
using boss::engines::LazyTransformation::utilities::isStaticValue;
using boss::engines::LazyTransformation::utilities::mergeConsecutiveSelectOperators;
using boss::expressions::CloneReason;
using boss::expressions::ComplexExpression;
using boss::expressions::generic::get;
using boss::expressions::generic::get_if;
using boss::expressions::generic::holds_alternative;
namespace boss {
using boss::expressions::atoms::Span;
};
using std::int64_t;

namespace {
std::vector<string> librariesToTest{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
}

// NOLINTBEGIN(readability-magic-numbers)
// NOLINTBEGIN(bugprone-exception-escape)
// NOLINTBEGIN(readability-function-cognitive-complexity)

TEST_CASE("IsCardinalityReducingOperator works correcty", "[utilities]") {
  CHECK(isCardinalityReducingOperator(boss::Symbol("Project")) == true);
  CHECK(isCardinalityReducingOperator(boss::Symbol("Select")) == true);
  CHECK(isCardinalityReducingOperator(boss::Symbol("Table")) == false);
}

TEST_CASE("IsStaticValue works correcty", "[utilities]") {
  CHECK(isStaticValue(boss::Expression(1)) == true);
  CHECK(isStaticValue(boss::Expression(1.0)) == true);
  CHECK(isStaticValue(boss::Expression(1.0f)) == true);
  CHECK(isStaticValue(boss::Expression("string")) == true);
  CHECK(isStaticValue(boss::Expression(true)) == true);
  CHECK(isStaticValue(boss::Expression("DateObject"_("2021-01-01"))) == true);
  CHECK(isStaticValue(boss::Expression(
            "Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3))), "Where"_("Equal"_("A"_, 1))))) == false);
}

TEST_CASE("IsInTransformationColumns works correctly", "[utilities]") {
  std::unordered_map<boss::Symbol, std::unordered_set<boss::Symbol>> transformationColumns{
      {"A"_, {}}, {"B"_, {}}, {"C"_, {}}};

  CHECK(isInTransformationColumns(transformationColumns, "A"_) == true);
  CHECK(isInTransformationColumns(transformationColumns, "B"_) == true);
  CHECK(isInTransformationColumns(transformationColumns, "C"_) == true);
  CHECK(isInTransformationColumns(transformationColumns, "D"_) == false);
}

TEST_CASE("IsConditionMoveable works corretly", "utilities") {
  std::unordered_map<boss::Symbol, std::unordered_set<boss::Symbol>> transformationColumns{
      {"A"_, {}}, {"B"_, {}}, {"C"_, {}}};

  std::unordered_set<boss::Symbol> usedSymbols = {};
  Expression inputExpression = ""_;
  CHECK(isConditionMoveable(inputExpression, "Equal"_("A"_, 1), transformationColumns, usedSymbols)[0] == true);
  CHECK(usedSymbols == std::unordered_set<boss::Symbol>{"A"_});
  CHECK(isConditionMoveable(inputExpression, "Greater"_("A"_, 1), transformationColumns, usedSymbols)[0] == true);
  CHECK(isConditionMoveable(inputExpression, "Equal"_("D"_, 1), transformationColumns, usedSymbols)[0] == false);
  CHECK(isConditionMoveable(inputExpression, "Equal"_("A"_, "D"_), transformationColumns, usedSymbols)[0] == false);
}

TEST_CASE("GetUsedSymbolsFromExpressions works correctly", "[utilities]") {
  Expression complexNestedExpression =
      "Project"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                    "Column"_("C"_, "List"_(7, 8, 9))),
                           "Where"_("Equal"_("A"_, 1))),
                 "As"_("D"_, "A"_, "E"_, "B"_, "F"_, "C"_));

  SECTION("Get all symbols") {
    Expression simpleExpression = "Equal"_("A"_, 1);
    std::unordered_set<boss::Symbol> usedSymbols = {};
    getUsedSymbolsFromExpressions(simpleExpression, usedSymbols);
    CHECK(usedSymbols == std::unordered_set<boss::Symbol>{"A"_});

    Expression complexExpression = "And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_));
    usedSymbols.clear();
    getUsedSymbolsFromExpressions(complexExpression, usedSymbols);
    CHECK(usedSymbols == std::unordered_set<boss::Symbol>{"A"_, "B"_, "C"_});

    usedSymbols.clear();
    getUsedSymbolsFromExpressions(complexNestedExpression, usedSymbols);
    CHECK(usedSymbols == std::unordered_set<boss::Symbol>{"A"_, "B"_, "C"_, "D"_, "E"_, "F"_});
  }

  SECTION("Get transformation symbols") {
    std::unordered_map<boss::Symbol, std::unordered_set<boss::Symbol>> transformationColumns{
        {"A"_, {}}, {"B"_, {}}, {"C"_, {}}};

    Expression simpleExpression = "Equal"_("A"_, 1);
    std::unordered_set<boss::Symbol> usedSymbols = {};
    getUsedSymbolsFromExpressions(simpleExpression, usedSymbols, transformationColumns);
    CHECK(usedSymbols == std::unordered_set<boss::Symbol>{"A"_});

    Expression complexExpression = "And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_));
    usedSymbols.clear();
    getUsedSymbolsFromExpressions(complexExpression, usedSymbols, transformationColumns);
    CHECK(usedSymbols == std::unordered_set<boss::Symbol>{"A"_, "B"_, "C"_});

    usedSymbols.clear();
    getUsedSymbolsFromExpressions(complexNestedExpression, usedSymbols, transformationColumns);
    CHECK(usedSymbols == std::unordered_set<boss::Symbol>{"A"_, "B"_, "C"_});
  }
}

TEST_CASE("GetUsedTransformationColumns works correctly", "[utilities]") {
  std::unordered_map<boss::Symbol, std::unordered_set<boss::Symbol>> transformationColumns{
      {"A"_, {}}, {"B"_, {}}, {"C"_, {}}};
  boss::Symbol UNEXCTRACTABLE = boss::engines::LazyTransformation::UNEXCTRACTABLE;

  Expression simpleExpression = "Equal"_("A"_, 1);
  std::unordered_set<boss::Symbol> usedTransformationColumns =
      getUsedTransformationColumns(simpleExpression, transformationColumns);
  CHECK(usedTransformationColumns == std::unordered_set<boss::Symbol>{"A"_});

  Expression complexExpression = "And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_));
  usedTransformationColumns = getUsedTransformationColumns(complexExpression, transformationColumns);
  CHECK(usedTransformationColumns == std::unordered_set<boss::Symbol>{"A"_, "B"_, "C"_});

  Expression complexNestedExpression =
      "Project"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                    "Column"_("C"_, "List"_(7, 8, 9))),
                           "Where"_("Equal"_("A"_, 1))),
                 "As"_("D"_, "A"_, "E"_, "B"_, "F"_, "C"_));

  usedTransformationColumns = getUsedTransformationColumns(complexNestedExpression, transformationColumns);
  CHECK(usedTransformationColumns == std::unordered_set<boss::Symbol>{"A"_, "B"_, "C"_, UNEXCTRACTABLE});
}

TEST_CASE("GetAllDependentSymbols works correctly", "[utilities]") {
  std::unordered_map<boss::Symbol, std::unordered_set<boss::Symbol>> transformationColumnsDependencies{
      {"A"_, {"D"_}}, {"B"_, {"C"_}}, {"C"_, {}}, {"D"_, {"E"_}}, {"E"_, {"F"_}}, {"H"_, {}}, {"K"_, {}}, {"L"_, {}}};

  std::unordered_set<boss::Symbol> usedSymbols = {"A"_, "B"_};
  std::unordered_set<boss::Symbol> dependentSymbols = getAllDependentSymbols(transformationColumnsDependencies, usedSymbols);
  CHECK(dependentSymbols == std::unordered_set<boss::Symbol>{"A"_, "B"_, "C"_, "D"_, "E"_, "F"_});
}

TEST_CASE("CanMoveConditionThroughProjection works correctly", "[utilities]") {
  ComplexExpression projectionOperator = "Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "As"_("D"_, "A"_, "E"_, "B"_, "F"_, "C"_));

  CHECK(canMoveConditionThroughProjection(projectionOperator, "Equal"_("A"_, 1)) == true);
  CHECK(canMoveConditionThroughProjection(projectionOperator, "Equal"_("D"_, 1)) == true);
  CHECK(canMoveConditionThroughProjection(projectionOperator, "Equal"_("A"_, "D"_)) == true);
  CHECK(canMoveConditionThroughProjection(projectionOperator, "Equal"_("X"_, 1)) == true);
}

TEST_CASE("IsOperationReversible works correctly", "[utilities]") { CHECK(true == isOperationReversible("A"_)); }

TEST_CASE("BuildColumnDependencies works correctly", "[utilities]") {
  ComplexExpression transformationExpression = "Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "As"_("D"_, "Times"_("A"_, "B"_), "P"_, "Plus"_("B"_, "C"_), "A"_, "A"_, "B"_, "B"_, "C"_, "C"_));

  std::unordered_map<boss::Symbol, std::unordered_set<boss::Symbol>> transformationColumns = {};
  std::unordered_set<boss::Symbol> untouchableColumns = {};
  buildColumnDependencies(transformationExpression, transformationColumns, untouchableColumns);

  CHECK(transformationColumns == std::unordered_map<boss::Symbol, std::unordered_set<boss::Symbol>>{
                                     {"D"_, {"A"_, "B"_}}, {"P"_, {"B"_, "C"_}}, {"A"_, {}}, {"B"_, {}}, {"C"_, {}}});

  ComplexExpression complexNestedTransformationWithTwoProjects = "Project"_(
      "Project"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                          "Column"_("C"_, "List"_(7, 8, 9))),
                 "As"_("D"_, "Times"_("A"_, "B"_), "P"_, "Plus"_("B"_, "C"_), "A"_, "A"_, "B"_, "B"_, "C"_, "C"_)),
      "As"_("X"_, "Plus"_("A"_, "Plus"_("B"_, "C"_)), "E"_, "Times"_("D"_, "P"_), "Q"_, "Plus"_("P"_, "D"_), "D"_, "D"_,
            "P"_, "P"_, "Q"_, "Q"_));

  transformationColumns.clear();
  untouchableColumns.clear();

  buildColumnDependencies(complexNestedTransformationWithTwoProjects, transformationColumns, untouchableColumns);

  CHECK(transformationColumns ==
        std::unordered_map<boss::Symbol, std::unordered_set<boss::Symbol>>{{"X"_, {"A"_, "B"_, "C"_}},
                                                                           {"E"_, {"D"_, "P"_}},
                                                                           {"Q"_, {"P"_, "D"_}},
                                                                           {"D"_, {"A"_, "B"_}},
                                                                           {"P"_, {"B"_, "C"_}},
                                                                           {"A"_, {}},
                                                                           {"B"_, {}},
                                                                           {"C"_, {}}});

  ComplexExpression transformationWithSelect = "Select"_(
      "Project"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                          "Column"_("C"_, "List"_(7, 8, 9))),
                 "As"_("D"_, "Times"_("A"_, "B"_), "P"_, "Plus"_("B"_, "C"_), "A"_, "A"_, "B"_, "B"_, "C"_, "C"_)),
      "Where"_("Equal"_("A"_, 1)));

  transformationColumns.clear();
  untouchableColumns.clear();

  buildColumnDependencies(transformationWithSelect, transformationColumns, untouchableColumns);

  CHECK(transformationColumns == std::unordered_map<boss::Symbol, std::unordered_set<boss::Symbol>>{
                                     {"D"_, {"A"_, "B"_}}, {"P"_, {"B"_, "C"_}}, {"A"_, {}}, {"B"_, {}}, {"C"_, {}}});
  CHECK(untouchableColumns == std::unordered_set<boss::Symbol>{"A"_});
}

TEST_CASE("MergeConsecutiveSelectOperators works correctly", "[utilities]") {
  ComplexExpression unchangeableSelect = "Select"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "Where"_("Equal"_("A"_, 1)));

  ComplexExpression mergedSelectOperator = mergeConsecutiveSelectOperators(std::move(unchangeableSelect));
  CHECK(mergedSelectOperator == "Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                                   "Column"_("C"_, "List"_(7, 8, 9))),
                                          "Where"_("Equal"_("A"_, 1))));

  ComplexExpression nestedSelect =
      "Select"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                   "Column"_("C"_, "List"_(7, 8, 9))),
                          "Where"_("Equal"_("A"_, 1))),
                "Where"_("Greater"_("B"_, "C"_)));

  ComplexExpression mergedComplexSelectOperator = mergeConsecutiveSelectOperators(std::move(nestedSelect));
  CHECK(mergedComplexSelectOperator ==
        "Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                           "Column"_("C"_, "List"_(7, 8, 9))),
                  "Where"_("And"_("Greater"_("B"_, "C"_), "Equal"_("A"_, 1)))));

  ComplexExpression nestedSelectWithOuterAnd =
      "Select"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                   "Column"_("C"_, "List"_(7, 8, 9))),
                          "Where"_("Equal"_("A"_, 1))),
                "Where"_("And"_("Equal"_("B"_, 4), "Equal"_("C"_, 7))));

  ComplexExpression mergedComplexSelectOperatorWithOuterAnd =
      mergeConsecutiveSelectOperators(std::move(nestedSelectWithOuterAnd));
  CHECK(mergedComplexSelectOperatorWithOuterAnd ==
        "Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                           "Column"_("C"_, "List"_(7, 8, 9))),
                  "Where"_("And"_("Equal"_("B"_, 4), "Equal"_("C"_, 7), "Equal"_("A"_, 1)))));

  ComplexExpression nestedSelectWithInnerAnd =
      "Select"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                   "Column"_("C"_, "List"_(7, 8, 9))),
                          "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("C"_, 7)))),
                "Where"_("Equal"_("B"_, 4)));

  ComplexExpression mergedComplexSelectOperatorWithInnerAnd =
      mergeConsecutiveSelectOperators(std::move(nestedSelectWithInnerAnd));
  CHECK(mergedComplexSelectOperatorWithInnerAnd ==
        "Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                           "Column"_("C"_, "List"_(7, 8, 9))),
                  "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("C"_, 7), "Equal"_("B"_, 4)))));

  ComplexExpression nestedSelectWithInnerAndOuterAnd =
      "Select"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                   "Column"_("C"_, "List"_(7, 8, 9))),
                          "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("C"_, 7)))),
                "Where"_("And"_("Equal"_("B"_, 4), "Equal"_("C"_, 8))));

  ComplexExpression mergedComplexSelectOperatorWithInnerAndOuterAnd =
      mergeConsecutiveSelectOperators(std::move(nestedSelectWithInnerAndOuterAnd));
  CHECK(mergedComplexSelectOperatorWithInnerAndOuterAnd ==
        "Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                           "Column"_("C"_, "List"_(7, 8, 9))),
                  "Where"_("And"_("Equal"_("B"_, 4), "Equal"_("C"_, 8), "Equal"_("A"_, 1), "Greater"_("C"_, 7)))));
}

TEST_CASE("AddConditionToWhereOperator works correctly", "[utilities]") {
  ComplexExpression simpleWhereOperator = "Where"_("Equal"_("A"_, 1));
  ComplexExpression conditionToAdd = "Greater"_("B"_, "C"_);

  ComplexExpression updatedWhereOperator = boss::engines::LazyTransformation::utilities::addConditionToWhereOperator(
      std::move(simpleWhereOperator), std::move(conditionToAdd));
  CHECK(updatedWhereOperator == "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_))));

  ComplexExpression complexWhereOperator = "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_)));
  ComplexExpression newConditionToAdd = "Equal"_("C"_, 9);
  ComplexExpression updatedComplexWhereOperator =
      boss::engines::LazyTransformation::utilities::addConditionToWhereOperator(std::move(complexWhereOperator),
                                                                                std::move(newConditionToAdd));

  CHECK(updatedComplexWhereOperator == "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_), "Equal"_("C"_, 9))));

  ComplexExpression complexWhereOperatorWithAnd = "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_)));
  ComplexExpression newConditionToAddWithAnd = "And"_("Equal"_("C"_, 9), "Greater"_("A"_, "D"_));
  ComplexExpression updatedComplexWhereOperatorWithAnd =
      boss::engines::LazyTransformation::utilities::addConditionToWhereOperator(std::move(complexWhereOperatorWithAnd),
                                                                                std::move(newConditionToAddWithAnd));
  CHECK(updatedComplexWhereOperatorWithAnd ==
        "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_), "Equal"_("C"_, 9), "Greater"_("A"_, "D"_))));
}

TEST_CASE("Extract operators from select works correctly") {
  ComplexExpression transformationExpression = "Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_));
  auto engine = boss::engines::LazyTransformation::Engine();

  std::vector<ComplexExpression> conditionsToMove = {};
  std::unordered_map<boss::Symbol, std::unordered_set<boss::Symbol>> dependencyColumns = {};
  std::unordered_set<boss::Symbol> usedColumns = {};
  std::unordered_set<boss::Symbol> untouchableColumns = {};
  buildColumnDependencies(transformationExpression, dependencyColumns, untouchableColumns);

  SECTION("Simple case") {
    ComplexExpression simpleSelectExpression = "Select"_("Table"_(), "Where"_("Equal"_("A"_, 1)));
    Expression updatedExpression = engine.extractOperatorsFromSelect(std::move(simpleSelectExpression),
                                                                     conditionsToMove, dependencyColumns, usedColumns);

    CHECK(conditionsToMove.size() == 1);
    CHECK(conditionsToMove[0] == "Equal"_("A"_, 1));
    CHECK(updatedExpression == "Table"_());
    CHECK(usedColumns == std::unordered_set<boss::Symbol>{"A"_});
  }

  SECTION("Complex case with And removal") {
    ComplexExpression complexSelectExpressionWithAndRemoval = "Select"_(
        "Table"_("Column"_("A"_, "List"_(1)), "Column"_("B"_, "List"_(2)), "Column"_("C"_, "List"_(3))),
        "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_), "Equal"_("C"_, 9), "Greater"_("A"_, "D"_))));

    Expression updatedExpression = engine.extractOperatorsFromSelect(std::move(complexSelectExpressionWithAndRemoval),
                                                                     conditionsToMove, dependencyColumns, usedColumns);
    CHECK(conditionsToMove.size() == 3);
    CHECK(conditionsToMove[0] == "Equal"_("A"_, 1));
    CHECK(conditionsToMove[1] == "Greater"_("B"_, "C"_));
    CHECK(conditionsToMove[2] == "Equal"_("C"_, 9));
    CHECK(updatedExpression ==
          "Select"_("Table"_("Column"_("A"_, "List"_(1)), "Column"_("B"_, "List"_(2)), "Column"_("C"_, "List"_(3))),
                    "Where"_("Greater"_("A"_, "D"_))));
    CHECK(usedColumns == std::unordered_set<boss::Symbol>{"A"_, "B"_, "C"_});
  }

  SECTION("Complex case with And remain") {
    ComplexExpression complexSelectExpressionWithAndRemain = "Select"_(
        "Table"_("Column"_("A"_, "List"_(1)), "Column"_("B"_, "List"_(2)), "Column"_("C"_, "List"_(3))),
        "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_), "Equal"_("D"_, 9), "Greater"_("A"_, "D"_))));

    Expression updatedExpression = engine.extractOperatorsFromSelect(std::move(complexSelectExpressionWithAndRemain),
                                                                     conditionsToMove, dependencyColumns, usedColumns);
    CHECK(conditionsToMove.size() == 2);
    CHECK(conditionsToMove[0] == "Equal"_("A"_, 1));
    CHECK(conditionsToMove[1] == "Greater"_("B"_, "C"_));
    CHECK(updatedExpression ==
          "Select"_("Table"_("Column"_("A"_, "List"_(1)), "Column"_("B"_, "List"_(2)), "Column"_("C"_, "List"_(3))),
                    "Where"_("And"_("Equal"_("D"_, 9), "Greater"_("A"_, "D"_)))));
    CHECK(usedColumns == std::unordered_set<boss::Symbol>{"A"_, "B"_, "C"_});
  }

  SECTION("Complex case with Or removal") {
    ComplexExpression complexSelectExpressionWithAndRemain =
        "Select"_("Table"_("Column"_("A"_, "List"_(1)), "Column"_("B"_, "List"_(2)), "Column"_("C"_, "List"_(3))),
                  "Where"_("Or"_("Equal"_("A"_, 1), "Greater"_("A"_, 2))));

    Expression updatedExpression = engine.extractOperatorsFromSelect(std::move(complexSelectExpressionWithAndRemain),
                                                                     conditionsToMove, dependencyColumns, usedColumns);
    CHECK(conditionsToMove.size() == 1);
    CHECK(conditionsToMove[0] == "Or"_("Equal"_("A"_, 1), "Greater"_("A"_, 2)));
    CHECK(updatedExpression ==
          "Table"_("Column"_("A"_, "List"_(1)), "Column"_("B"_, "List"_(2)), "Column"_("C"_, "List"_(3))));
  }

  SECTION("Complex case with Or remain") {
    ComplexExpression complexSelectExpressionWithAndRemain =
        "Select"_("Table"_("Column"_("A"_, "List"_(1)), "Column"_("B"_, "List"_(2)), "Column"_("C"_, "List"_(3))),
                  "Where"_("Or"_("And"_("Equal"_("A"_, 1), "Greater"_("C"_, 6)), "Greater"_("A"_, 2))));

    Expression updatedExpression = engine.extractOperatorsFromSelect(std::move(complexSelectExpressionWithAndRemain),
                                                                     conditionsToMove, dependencyColumns, usedColumns);
    CHECK(conditionsToMove.size() == 1);
    CHECK(conditionsToMove[0] == "Or"_("Equal"_("A"_, 1), "Greater"_("A"_, 2)));
    CHECK(updatedExpression ==
          "Select"_("Table"_("Column"_("A"_, "List"_(1)), "Column"_("B"_, "List"_(2)), "Column"_("C"_, "List"_(3))),
                    "Where"_("Greater"_("C"_, 6))));
  }

  SECTION("Extract with Union inside") {
    ComplexExpression complexSelectExpressionWithUnion =
        "Select"_("Union"_("Transformation"_,
                           "Table"_("Column"_("A"_, "List"_(1)), "Column"_("B"_, "List"_(2)), "Column"_("C"_, "List"_(3)))),
                  "Where"_("Equal"_("A"_, 1)));
    Expression updatedExpression = engine.extractOperatorsFromSelect(std::move(complexSelectExpressionWithUnion),
                                                                     conditionsToMove, dependencyColumns, usedColumns);
    CHECK(conditionsToMove.size() == 1);
    CHECK(conditionsToMove[0] == "Equal"_("A"_, 1));
    CHECK(updatedExpression ==
          "Union"_("Transformation"_,
                   "Select"_("Table"_("Column"_("A"_, "List"_(1)), "Column"_("B"_, "List"_(2)), "Column"_("C"_, "List"_(3))),
                             "Where"_("Equal"_("A"_, 1)))));
  }
}

TEST_CASE("MoveExctractedSelectExpressionToTransformation works correctly") {
  SECTION("Projection case") {
    ComplexExpression projectTransformationExpression = "Project"_(
        "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
        "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_));

    ComplexExpression simpleEqualExpression = "Equal"_("A"_, 1);
    std::unordered_set<boss::Symbol> usedColumns = {"A"_};

    ComplexExpression updatedTransformationExpression = moveExctractedSelectExpressionToTransformation(
        std::move(projectTransformationExpression), std::move(simpleEqualExpression), usedColumns);
    CHECK(updatedTransformationExpression ==
          "Project"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                        "Column"_("C"_, "List"_(7, 8, 9))),
                               "Where"_("Equal"_("A"_, 1))),
                     "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_)));
  }

  SECTION("Propagate through Grouped case") {
    ComplexExpression groupedTransformationExpression = "Group"_(
        "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
        "By"_("A"_, "B"_, "C"_), "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_));

    ComplexExpression complexEqualExpression = "And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_));
    std::unordered_set<boss::Symbol> usedColumns = {"A"_, "B"_, "C"_};

    ComplexExpression updatedTransformationExpression = moveExctractedSelectExpressionToTransformation(
        std::move(groupedTransformationExpression), std::move(complexEqualExpression), usedColumns);
    CHECK(updatedTransformationExpression ==
          "Group"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                      "Column"_("C"_, "List"_(7, 8, 9))),
                             "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_)))),
                   "By"_("A"_, "B"_, "C"_), "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_)));
  }

  SECTION("Doesn't propagate through Grouped case") {
    ComplexExpression groupedTransformationExpression = "Group"_(
        "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("D"_, "List"_(7, 8, 9))),
        "By"_("A"_, "B"_, "C"_), "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_));

    ComplexExpression complexEqualExpression = "And"_("Equal"_("A"_, 1), "Greater"_("D"_, 3));
    std::unordered_set<boss::Symbol> usedColumns = {"A"_, "D"_};

    ComplexExpression updatedTransformationExpression = moveExctractedSelectExpressionToTransformation(
        std::move(groupedTransformationExpression), std::move(complexEqualExpression), usedColumns);
    CHECK(updatedTransformationExpression ==
          "Select"_("Group"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                      "Column"_("D"_, "List"_(7, 8, 9))),
                             "By"_("A"_, "B"_, "C"_), "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_)),
                    "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("D"_, 3)))));
  }

  SECTION("Propagate through Join first input") {
    ComplexExpression joinTransformationExpression = "Join"_(
        "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
        "Table"_("Column"_("D"_, "List"_(1, 2, 3)), "Column"_("E"_, "List"_(4, 5, 6)), "Column"_("F"_, "List"_(7, 8, 9))),
        "Where"_("Equal"_("A"_, "D"_)));

    ComplexExpression complexEqualExpression = "And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_));
    std::unordered_set<boss::Symbol> usedColumns = {"A"_, "B"_, "C"_};

    ComplexExpression updatedTransformationExpression = moveExctractedSelectExpressionToTransformation(
        std::move(joinTransformationExpression), std::move(complexEqualExpression), usedColumns);
    CHECK(updatedTransformationExpression ==
          "Join"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                     "Column"_("C"_, "List"_(7, 8, 9))),
                            "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_)))),
                  "Table"_("Column"_("D"_, "List"_(1, 2, 3)), "Column"_("E"_, "List"_(4, 5, 6)),
                           "Column"_("F"_, "List"_(7, 8, 9))),
                  "Where"_("Equal"_("A"_, "D"_))));
  }

  SECTION("Propagate through Join second input") {
    ComplexExpression joinTransformationExpression = "Join"_(
        "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
        "Table"_("Column"_("D"_, "List"_(1, 2, 3)), "Column"_("E"_, "List"_(4, 5, 6)), "Column"_("F"_, "List"_(7, 8, 9))),
        "Where"_("Equal"_("A"_, "D"_)));

    ComplexExpression complexEqualExpression = "And"_("Equal"_("D"_, 1), "Greater"_("E"_, "F"_));
    std::unordered_set<boss::Symbol> usedColumns = {"D"_, "E"_, "F"_};

    ComplexExpression updatedTransformationExpression = moveExctractedSelectExpressionToTransformation(
        std::move(joinTransformationExpression), std::move(complexEqualExpression), usedColumns);
    CHECK(updatedTransformationExpression ==
          "Join"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                           "Column"_("C"_, "List"_(7, 8, 9))),
                  "Select"_("Table"_("Column"_("D"_, "List"_(1, 2, 3)), "Column"_("E"_, "List"_(4, 5, 6)),
                                     "Column"_("F"_, "List"_(7, 8, 9))),
                            "Where"_("And"_("Equal"_("D"_, 1), "Greater"_("E"_, "F"_)))),
                  "Where"_("Equal"_("A"_, "D"_))));
  }

  SECTION("Doesn't propagate through Join case") {
    ComplexExpression joinTransformationExpression = "Join"_(
        "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
        "Table"_("Column"_("D"_, "List"_(1, 2, 3)), "Column"_("E"_, "List"_(4, 5, 6)), "Column"_("F"_, "List"_(7, 8, 9))),
        "Where"_("Equal"_("A"_, "D"_)));

    ComplexExpression complexEqualExpression = "And"_("Equal"_("A"_, 1), "Greater"_("D"_, 3));
    std::unordered_set<boss::Symbol> usedColumns = {"A"_, "D"_};

    ComplexExpression updatedTransformationExpression = moveExctractedSelectExpressionToTransformation(
        std::move(joinTransformationExpression), std::move(complexEqualExpression), usedColumns);
    CHECK(updatedTransformationExpression ==
          "Select"_("Join"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                     "Column"_("C"_, "List"_(7, 8, 9))),
                            "Table"_("Column"_("D"_, "List"_(1, 2, 3)), "Column"_("E"_, "List"_(4, 5, 6)),
                                     "Column"_("F"_, "List"_(7, 8, 9))),
                            "Where"_("Equal"_("A"_, "D"_))),
                    "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("D"_, 3)))));
  }

  SECTION("Propagate through Union first input") {
    ComplexExpression unionTransformationExpression = "Union"_(
        "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
        "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))));

    ComplexExpression complexEqualExpression = "And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_));
    std::unordered_set<boss::Symbol> usedColumns = {"A"_, "B"_, "C"_};

    ComplexExpression updatedTransformationExpression = moveExctractedSelectExpressionToTransformation(
        std::move(unionTransformationExpression), std::move(complexEqualExpression), usedColumns);
    CHECK(updatedTransformationExpression ==
          "Union"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                      "Column"_("C"_, "List"_(7, 8, 9))),
                             "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_)))),
                   "Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                      "Column"_("C"_, "List"_(7, 8, 9))),
                             "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_))))));
  }
}

TEST_CASE("RemoveUnusedTransformationColumns works correctly") {
  ComplexExpression transformationExpression = "Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_));

  std::unordered_set<boss::Symbol> usedSymbols = {"A"_, "B"_};
  std::unordered_set<boss::Symbol> untouchableColumns = {};
  ComplexExpression updatedTransformationExpression =
      removeUnusedTransformationColumns(std::move(transformationExpression), usedSymbols, untouchableColumns);
  CHECK(updatedTransformationExpression ==
        "Project"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                            "Column"_("C"_, "List"_(7, 8, 9))),
                   "As"_("A"_, "A"_, "B"_, "B"_)));
};

TEST_CASE("ReplaceTransformSymbolsWithQuery") {
  ComplexExpression transformationExpression = "Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_));

  Expression userExpression = "Select"_("Transformation"_, "Where"_("Equal"_("A"_, 1)));

  auto engine = boss::engines::LazyTransformation::Engine();

  Expression updatedUserExpression =
      replaceTransformSymbolsWithQuery(std::move(userExpression), std::move(transformationExpression));

  CHECK(updatedUserExpression ==
        "Select"_("Project"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                      "Column"_("C"_, "List"_(7, 8, 9))),
                             "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_)),
                  "Where"_("Equal"_("A"_, 1))));
}

TEST_CASE("AddTransformation works correctly") {
  ComplexExpression transformationExpression = "AddTransformation"_("Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_)));

  auto engine = boss::engines::LazyTransformation::Engine();

  Expression result = engine.evaluate(std::move(transformationExpression));
  Expression expected = "Transformation added successfully"_;
  CHECK(result == expected);
}

TEST_CASE("GetTransformation works correctly") {
  ComplexExpression firstTransformation = "AddTransformation"_("Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_)));

  auto engine = boss::engines::LazyTransformation::Engine();

  Expression result = engine.evaluate(std::move(firstTransformation));
  Expression expected = "Transformation added successfully"_;
  CHECK(result == expected);

  Expression getTransformationExpression = "GetTransformation"_();
  Expression transformation = engine.evaluate(std::move(getTransformationExpression));
  CHECK(transformation == "Project"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                              "Column"_("C"_, "List"_(7, 8, 9))),
                                     "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_)));

  ComplexExpression secondTransformation = "AddTransformation"_("Project"_(
      "Table"_("Column"_("D"_, "List"_(1, 2, 3)), "Column"_("E"_, "List"_(4, 5, 6)), "Column"_("F"_, "List"_(7, 8, 9))),
      "As"_("D"_, "D"_, "E"_, "E"_, "F"_, "F"_)));

  result = engine.evaluate(std::move(secondTransformation));
  CHECK(result == expected);

  getTransformationExpression = "GetTransformation"_(1);
  transformation = engine.evaluate(std::move(getTransformationExpression));
  CHECK(transformation == "Project"_("Table"_("Column"_("D"_, "List"_(1, 2, 3)), "Column"_("E"_, "List"_(4, 5, 6)),
                                              "Column"_("F"_, "List"_(7, 8, 9))),
                                     "As"_("D"_, "D"_, "E"_, "E"_, "F"_, "F"_)));
}

TEST_CASE("RemoveTransformation works correctly") {
  ComplexExpression addTransformationExpression = "AddTransformation"_("Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_)));

  auto engine = boss::engines::LazyTransformation::Engine();
  Expression addedSuccessfully = "Transformation added successfully"_;
  Expression removedSuccessfully = "Transformation removed successfully"_;

  Expression outOfBounds = "Error"_("Transformation index out of bounds"_);

  SECTION("Remove empty transformation") {
    Expression removeTransformationExpression = "RemoveTransformation"_();
    Expression removeResult = engine.evaluate(std::move(removeTransformationExpression));
    CHECK(removeResult == removedSuccessfully);
  }

  Expression result = engine.evaluate(std::move(addTransformationExpression));
  CHECK(result == addedSuccessfully);

  SECTION("Incorrect remove case") {
    Expression incorrectRemoveTransformationExpression = "RemoveTransformation"_(1);
    Expression incorrectRemoveResult = engine.evaluate(std::move(incorrectRemoveTransformationExpression));

    CHECK(incorrectRemoveResult == outOfBounds);
  }

  SECTION("Correct remove case with number") {
    Expression removeTransformationExpression = "RemoveTransformation"_(0);
    Expression removeResult = engine.evaluate(std::move(removeTransformationExpression));
    CHECK(removeResult == removedSuccessfully);
  }

  SECTION("Correct remove case without number") {
    Expression removeTransformationExpression = "RemoveTransformation"_();
    Expression removeResult = engine.evaluate(std::move(removeTransformationExpression));
    CHECK(removeResult == removedSuccessfully);
  }

  SECTION("Remove all") {
    Expression removeTransformationExpression = "RemoveAllTransformations"_();
    Expression removeResult = engine.evaluate(std::move(removeTransformationExpression));
    Expression expected = "All transformations removed successfully"_;
    CHECK(removeResult == expected);
  }
}

TEST_CASE("Evaluate works correctly") {
  ComplexExpression transformationExpression = "AddTransformation"_("Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_)));
  auto engine = boss::engines::LazyTransformation::Engine();
  engine.evaluate(std::move(transformationExpression));

  SECTION("Simple case") {
    Expression userExpression = "ApplyTransformation"_(
        "Select"_("Transformation"_, "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_)))));
    Expression updatedUserExpression = engine.evaluate(std::move(userExpression));

    CHECK(updatedUserExpression ==
          "Project"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                        "Column"_("C"_, "List"_(7, 8, 9))),
                               "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_)))),
                     "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_)));
  }

  SECTION("Complex case") {
    Expression userExpression =
        "ApplyTransformation"_("Select"_("Select"_("Project"_("Transformation"_, "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_)),
                                                   "Where"_("And"_("Greater"_("A"_, 2), "Greater"_("B"_, "C"_)))),
                                         "Where"_("Equal"_("A"_, 8))));
    Expression updatedUserExpression = engine.evaluate(std::move(userExpression));

    CHECK(updatedUserExpression ==
          "Project"_("Project"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                                   "Column"_("C"_, "List"_(7, 8, 9))),
                                          "Where"_("And"_("Greater"_("A"_, 2), "Greater"_("B"_, "C"_), "Equal"_("A"_, 8)))),
                                "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_)),
                     "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_)));
  }
}

TEST_CASE("Line") {
  auto transform = "AddTransformation"_("GroupBy"_(
      "Select"_("Project"_(
                    "Project"_("LINEITEM"_, "As"_("l_newcurrencyextendedprice"_, "Times"_("l_extendedprice"_, 1.1), "l_tax"_,
                                                  "l_tax"_, "discount"_, "discount"_, "l_returnflag"_, "l_returnflag"_,
                                                  "l_linestatus"_, "l_linestatus"_, "l_partkey"_, "l_partkey"_)),
                    "As"_("profit"_, "Times"_("l_newcurrencyextendedprice"_, "Minus"_(1, "Plus"_("l_tax"_, "l_discount"_))),
                          "l_returnflag"_, "l_returnflag"_, "l_linestatus"_, "l_linestatus"_, "l_partkey"_, "l_partkey"_,
                          "l_newcurrencyextendedprice"_, "l_newcurrencyextendedprice"_)),
                "Where"_("Equal"_("l_returnflag"_, 1))),
      "By"_("l_partkey"_, "l_linestatus"_),
      "As"_("sum_extendedprice"_, "Sum"_("l_newcurrencyextendedprice"_), "sum_profit"_, "Sum"_("profit"_))));

  auto apply1 = "ApplyTransformation"_(
      "GroupBy"_("Select"_("Transformation"_, "Where"_("And"_("Equal"_("l_linestatus"_, 0), "Greater"_(1, "l_partkey"_)))),
                 "By"_("l_partkey"_),
                 "As"_("sum_sum_extended_price"_, "Sum"_("sum_extendedprice"_), "sum__sum_profit"_, "Sum"_("sum_profit"_))));

  auto apply2 = "ApplyTransformation"_(
      "GroupBy"_("Select"_("Transformation"_, "Where"_("And"_("Equal"_("l_linestatus"_, 1), "Greater"_(1, "l_partkey"_)))),
                 "By"_("l_partkey"_),
                 "As"_("sum_sum_extended_price"_, "Sum"_("sum_extendedprice"_), "sum__sum_profit"_, "Sum"_("sum_profit"_))));

  auto engine = boss::engines::LazyTransformation::Engine();
  engine.evaluate(std::move(transform));
  auto answer1 = engine.evaluate(std::move(apply1));
  CHECK(answer1 ==
        "GroupBy"_(
            "GroupBy"_(
                "Select"_(
                    "Project"_("Project"_("Select"_("LINEITEM"_, "Where"_("And"_("Equal"_("l_linestatus"_, 0),
                                                                                 "Greater"_(1, "l_partkey"_)))),
                                          "As"_("l_newcurrencyextendedprice"_, "Times"_("l_extendedprice"_, 1.1), "l_tax"_,
                                                "l_tax"_, "l_returnflag"_, "l_returnflag"_, "l_linestatus"_, "l_linestatus"_,
                                                "l_partkey"_, "l_partkey"_)),
                               "As"_("profit"_,
                                     "Times"_("l_newcurrencyextendedprice"_, "Minus"_(1, "Plus"_("l_tax"_, "l_discount"_))),
                                     "l_returnflag"_, "l_returnflag"_, "l_linestatus"_, "l_linestatus"_, "l_partkey"_,
                                     "l_partkey"_, "l_newcurrencyextendedprice"_, "l_newcurrencyextendedprice"_)),
                    "Where"_("Equal"_("l_returnflag"_, 1))),
                "By"_("l_partkey"_, "l_linestatus"_),
                "As"_("sum_extendedprice"_, "Sum"_("l_newcurrencyextendedprice"_), "sum_profit"_, "Sum"_("profit"_))),
            "By"_("l_partkey"_),
            "As"_("sum_sum_extended_price"_, "Sum"_("sum_extendedprice"_), "sum__sum_profit"_, "Sum"_("sum_profit"_))));
}

TEST_CASE("Balanced butterfly") {
  SECTION("Transform") {
    auto transform = "AddTransformation"_(
        "Join"_("Project"_("PARTSUPP"_, "As"_("ps_partkey"_, "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_, "ps_availqty"_,
                                              "ps_availqty"_, "ps_supplycost"_, "ps_supplycost"_, "ps_total_cost"_,
                                              "Times"_("ps_supplycost"_, "ps_availqty"_), "ps_comment"_, "ps_comment"_)),
                "Project"_("SUPPLIER"_, "As"_("s_suppkey"_, "s_suppkey"_, "s_name"_, "s_name"_, "s_address"_, "s_address"_,
                                              "s_nationkey"_, "s_nationkey"_, "s_phone"_, "s_phone"_, "s_acctbalcurrency"_,
                                              "Times"_("s_acctbal"_, 1.1), "s_comment"_, "s_comment"_)),
                "Where"_("Equal"_("s_suppkey"_, "ps_suppkey"_))));

    auto apply = "ApplyTransformation"_("Group"_(
        "Group"_("Select"_("Transformation"_, "Where"_("Greater"_(1, "ps_partkey"_))), "By"_("ps_partkey"_, "s_nationkey"_),
                 "As"_("max_supplycost"_, "Max"_("ps_supplycost"_), "min_supplycost"_, "Min"_("ps_supplycost"_))),
        "By"_("ps_partkey"_),
        "As"_("max_max_supplycost"_, "Max"_("max_supplycost"_), "min_min_supplycost"_, "Min"_("min_supplycost"_))));

    auto engine = boss::engines::LazyTransformation::Engine();
    engine.evaluate(std::move(transform));
    auto answer = engine.evaluate(std::move(apply));
    CHECK(
        answer ==
        "Group"_("Group"_("Join"_("Project"_("Select"_("PARTSUPP"_, "Where"_("Greater"_(1, "ps_partkey"_))),
                                             "As"_("ps_partkey"_, "ps_partkey"_, "ps_suppkey"_, "ps_suppkey"_,
                                                   "ps_supplycost"_, "ps_supplycost"_)),
                                  "Project"_("SUPPLIER"_, "As"_("s_suppkey"_, "s_suppkey"_, "s_nationkey"_, "s_nationkey"_)),
                                  "Where"_("Equal"_("s_suppkey"_, "ps_suppkey"_))),
                          "By"_("ps_partkey"_, "s_nationkey"_),
                          "As"_("max_supplycost"_, "Max"_("ps_supplycost"_), "min_supplycost"_, "Min"_("ps_supplycost"_))),
                 "By"_("ps_partkey"_),
                 "As"_("max_max_supplycost"_, "Max"_("max_supplycost"_), "min_min_supplycost"_, "Min"_("min_supplycost"_))));
  }
}

int main(int argc, char *argv[]) {
  Catch::Session session;
  session.cli(session.cli() | Catch::clara::Opt(librariesToTest, "library")["--library"]);
  auto const returnCode = session.applyCommandLine(argc, argv);
  if (returnCode != 0) {
    return returnCode;
  }
  return session.run();
}
