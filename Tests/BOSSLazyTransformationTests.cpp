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

// #include "../Source/BOSSLazyTransformationEngine.cpp"
#include "../Source/BOSSLazyTransformationEngine.hpp"

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
  std::unordered_set<boss::Symbol> transformationColumns{"A"_, "B"_, "C"_};

  CHECK(isInTransformationColumns(transformationColumns, "A"_) == true);
  CHECK(isInTransformationColumns(transformationColumns, "B"_) == true);
  CHECK(isInTransformationColumns(transformationColumns, "C"_) == true);
  CHECK(isInTransformationColumns(transformationColumns, "D"_) == false);
}

TEST_CASE("IsConditionMoveable works corretly", "utilities") {
  std::unordered_set<boss::Symbol> transformationColumns{"A"_, "B"_, "C"_};

  CHECK(isConditionMoveable("Equal"_("A"_, 1), transformationColumns) == true);
  CHECK(isConditionMoveable("Greater"_("A"_, 1), transformationColumns) == true);
  CHECK(isConditionMoveable("Equal"_("D"_, 1), transformationColumns) == false);
  CHECK(isConditionMoveable("Equal"_("A"_, "D"_), transformationColumns) == false);
}

TEST_CASE("GetUsedSymbolsFromExpressions works correctly", "[utilities]") {
  Expression complexNestedExpression =
      "Project"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                    "Column"_("C"_, "List"_(7, 8, 9))),
                           "Where"_("Equal"_("A"_, 1))),
                 "As"_("D"_, "A"_, "E"_, "B_", "F"_, "C"_));

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
    std::unordered_set<boss::Symbol> transformationColumns{"A"_, "B"_, "C"_};

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
  std::unordered_set<boss::Symbol> transformationColumns{"A"_, "B"_, "C"_};
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
                 "As"_("D"_, "A"_, "E"_, "B_", "F"_, "C"_));

  usedTransformationColumns = getUsedTransformationColumns(complexNestedExpression, transformationColumns);
  CHECK(usedTransformationColumns == std::unordered_set<boss::Symbol>{"A"_, "B"_, "C"_, UNEXCTRACTABLE});
}

TEST_CASE("IsOperationReversible works correctly", "[utilities]") { CHECK(true == isOperationReversible("A"_)); }

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

TEST_CASE("ExtractTransformationColumns works correctly") {
  ComplexExpression transformationExpression = "Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6))), "As"_("A"_, "A"_, "B"_, "B"_));

  std::unordered_set<boss::Symbol> transformationColumns{"A"_, "B"_};
  boss::engines::LazyTransformation::Engine engine(std::move(transformationExpression));
  auto extractedColumns = engine.extractTransformationColumns();
  CHECK(extractedColumns == transformationColumns);
}

TEST_CASE("Extract operators from select works correctly") {
  ComplexExpression transformationExpression = "Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "As"_("A"_, "A"_, "B"_, "B_", "C"_, "C"_));
  boss::engines::LazyTransformation::Engine engine(std::move(transformationExpression));

  ComplexExpression simpleSelectExpression = "Select"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "Where"_("Equal"_("A"_, 1)));

  std::vector<ComplexExpression> conditionsToMove = {};
  std::unordered_set<boss::Symbol> projectionColumns = {};

  Expression updatedExpression =
      engine.extractOperatorsFromSelect(std::move(simpleSelectExpression), conditionsToMove, projectionColumns);

  CHECK(conditionsToMove.size() == 1);
  CHECK(conditionsToMove[0] == "Equal"_("A"_, 1));
  CHECK(updatedExpression == "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                      "Column"_("C"_, "List"_(7, 8, 9))));

  ComplexExpression complexSelectExpressionWithAndRemoval =
      "Select"_("Table"_("Column"_("A"_, "List"_(1)), "Column"_("B"_, "List"_(2)), "Column"_("C"_, "List"_(3))),
                "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_), "Equal"_("C"_, 9), "Greater"_("A"_, "D"_))));

  conditionsToMove.clear();
  projectionColumns.clear();
  updatedExpression = engine.extractOperatorsFromSelect(std::move(complexSelectExpressionWithAndRemoval),
                                                        conditionsToMove, projectionColumns);
  CHECK(conditionsToMove.size() == 3);
  CHECK(conditionsToMove[0] == "Equal"_("A"_, 1));
  CHECK(conditionsToMove[1] == "Greater"_("B"_, "C"_));
  CHECK(conditionsToMove[2] == "Equal"_("C"_, 9));
  CHECK(updatedExpression ==
        "Select"_("Table"_("Column"_("A"_, "List"_(1)), "Column"_("B"_, "List"_(2)), "Column"_("C"_, "List"_(3))),
                  "Where"_("Greater"_("A"_, "D"_))));

  ComplexExpression complexSelectExpressionWithAndRemain =
      "Select"_("Table"_("Column"_("A"_, "List"_(1)), "Column"_("B"_, "List"_(2)), "Column"_("C"_, "List"_(3))),
                "Where"_("And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_), "Equal"_("D"_, 9), "Greater"_("A"_, "D"_))));

  conditionsToMove.clear();
  projectionColumns.clear();
  updatedExpression = engine.extractOperatorsFromSelect(std::move(complexSelectExpressionWithAndRemain),
                                                        conditionsToMove, projectionColumns);
  CHECK(conditionsToMove.size() == 2);
  CHECK(conditionsToMove[0] == "Equal"_("A"_, 1));
  CHECK(conditionsToMove[1] == "Greater"_("B"_, "C"_));
  CHECK(updatedExpression ==
        "Select"_("Table"_("Column"_("A"_, "List"_(1)), "Column"_("B"_, "List"_(2)), "Column"_("C"_, "List"_(3))),
                  "Where"_("And"_("Equal"_("D"_, 9), "Greater"_("A"_, "D"_)))));
}

TEST_CASE("MoveExctractedSelectExpressionToTransformation works correctly") {
  ComplexExpression transformationExpression = "Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_));

  ComplexExpression simpleEqualExpression = "Equal"_("A"_, 1);

  ComplexExpression updatedTransformationExpression = moveExctractedSelectExpressionToTransformation(
      std::move(transformationExpression), std::move(simpleEqualExpression));
  CHECK(updatedTransformationExpression ==
        "Project"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                      "Column"_("C"_, "List"_(7, 8, 9))),
                             "Where"_("Equal"_("A"_, 1))),
                   "As"_("A"_, "A"_, "B"_, "B"_, "C"_, "C"_)));
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
