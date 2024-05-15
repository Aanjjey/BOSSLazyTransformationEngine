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
using boss::engines::LazyTransformation::utilities::getUsedSymbolsFromExpressions;
using boss::engines::LazyTransformation::utilities::isCardinalityReducingOperator;
using boss::engines::LazyTransformation::utilities::isConditionMoveable;
using boss::engines::LazyTransformation::utilities::isInTransformationColumns;
using boss::engines::LazyTransformation::utilities::isOperationReversible;
using boss::engines::LazyTransformation::utilities::isStaticValue;
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
TEST_CASE("Subspans work correctly", "[spans]") { CHECK(1 == 1); }

TEST_CASE("Extraction of transformation columns works correctly") {
  ComplexExpression transformationExpression = "Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6))), "As"_("A"_, "A"_, "B"_, "B"_));

  std::unordered_set<boss::Symbol> transformationColumns{"A"_, "B"_};
  boss::engines::LazyTransformation::Engine engine(std::move(transformationExpression));
  auto extractedColumns = engine.extractTransformationColumns();
  CHECK(extractedColumns == transformationColumns);
}

TEST_CASE("Is in transformation columns works correctly") {
  ComplexExpression transformationExpression = "Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "As"_("A"_, "A"_, "B"_, "B_", "C"_, "C"_));

  std::unordered_set<boss::Symbol> transformationColumns{"A"_, "B"_, "C"_};
  boss::engines::LazyTransformation::Engine engine(std::move(transformationExpression));
  auto extractedColumns = engine.extractTransformationColumns();
  CHECK(extractedColumns == transformationColumns);

  CHECK(true == isInTransformationColumns(transformationColumns, "A"_));
  CHECK(true == isInTransformationColumns(transformationColumns, "B"_));
  CHECK(true == isInTransformationColumns(transformationColumns, "C"_));
  CHECK(false == isInTransformationColumns(transformationColumns, "D"_));
}

TEST_CASE("Is condition moveable works correctly") {
  ComplexExpression transformationExpression = "Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "As"_("A"_, "A"_, "B"_, "B_", "C"_, "C"_));
  boss::engines::LazyTransformation::Engine engine(std::move(transformationExpression));
  const std::unordered_set<boss::Symbol> transformationColumns{"A"_, "B"_, "C"_};

  const ComplexExpression simpleEquality = "Equal"_("A"_, 1);
  const ComplexExpression simpleGreaterThan = "Greater"_("A"_, 1);
  const ComplexExpression equalityWithIncorrectColumn = "Equal"_("D"_, 1);
  const ComplexExpression equalityWithOneCorrectColumnOneIncorrectColumns = "Equal"_("A"_, "D"_);

  CHECK(true == isConditionMoveable(simpleEquality, transformationColumns));
  CHECK(true == isConditionMoveable(simpleGreaterThan, transformationColumns));
  CHECK(false == isConditionMoveable(equalityWithIncorrectColumn, transformationColumns));
  CHECK(false == isConditionMoveable(equalityWithOneCorrectColumnOneIncorrectColumns, transformationColumns));
}

TEST_CASE("Get used symbols from expressions works correctly") {
  ComplexExpression transformationExpression = "Project"_(
      "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)), "Column"_("C"_, "List"_(7, 8, 9))),
      "As"_("A"_, "A"_, "B"_, "B_", "C"_, "C"_));
  boss::engines::LazyTransformation::Engine engine(std::move(transformationExpression));
  std::unordered_set<boss::Symbol> transformationColumns = {"A"_, "B"_, "C"_};

  ComplexExpression simpleEquality = "Equal"_("A"_, 1);
  ComplexExpression simpleGreaterThan = "Greater"_("B"_, "C"_);
  ComplexExpression complexAnd = "And"_("Equal"_("A"_, 1), "Greater"_("B"_, "C"_));
  ComplexExpression complexNestedExpression =
      "Project"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6)),
                                    "Column"_("C"_, "List"_(7, 8, 9))),
                           "Where"_("Equal"_("A"_, 1))),
                 "As"_("D"_, "A"_, "E"_, "B_", "F"_, "C"_));

  std::unordered_set<boss::Symbol> simpleEqualityUsedSymbols = {};
  std::unordered_set<boss::Symbol> simpleGreaterThanUsedSymbols = {};
  std::unordered_set<boss::Symbol> complexAndUsedSymbols = {};
  std::unordered_set<boss::Symbol> complexNestedExpressionUsedSymbols = {};

  getUsedSymbolsFromExpressions(std::move(simpleEquality), simpleEqualityUsedSymbols, transformationColumns);
  getUsedSymbolsFromExpressions(std::move(simpleGreaterThan), simpleGreaterThanUsedSymbols, transformationColumns);
  getUsedSymbolsFromExpressions(std::move(complexAnd), complexAndUsedSymbols, transformationColumns);
  getUsedSymbolsFromExpressions(std::move(complexNestedExpression), complexNestedExpressionUsedSymbols,
                                transformationColumns);

  CHECK(simpleEqualityUsedSymbols == std::unordered_set<boss::Symbol>{"A"_});
  CHECK(simpleGreaterThanUsedSymbols == std::unordered_set<boss::Symbol>{"B"_, "C"_});
  CHECK(complexAndUsedSymbols == std::unordered_set<boss::Symbol>{"A"_, "B"_, "C"_});
  // The nested expression should only return the symbols that are used in the transformation expression
  CHECK(complexNestedExpressionUsedSymbols == std::unordered_set<boss::Symbol>{"A"_, "B"_, "C"_});
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

int main(int argc, char *argv[]) {
  Catch::Session session;
  session.cli(session.cli() | Catch::clara::Opt(librariesToTest, "library")["--library"]);
  auto const returnCode = session.applyCommandLine(argc, argv);
  if (returnCode != 0) {
    return returnCode;
  }
  return session.run();
}
