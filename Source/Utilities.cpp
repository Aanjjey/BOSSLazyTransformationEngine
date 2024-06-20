#include "Utilities.hpp"

#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <iostream>
#include <mutex>
#include <typeinfo>
#include <unordered_set>
#include <variant>
#include <vector>

#include "BOSSLazyTransformationEngine.hpp"

using std::string_literals::operator""s;
using boss::engines::LazyTransformation::UNEXCTRACTABLE;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;

using boss::Expression;

namespace boss::engines::LazyTransformation::utilities {

bool isCardinalityReducingOperator(const Symbol& op) {
  static const std::unordered_set<Symbol> cardinalityReducingOps = {"Select"_, "Project"_};
  return cardinalityReducingOps.find(op) != cardinalityReducingOps.end();
}

bool supportedOperator(const Symbol& op) {
  static const std::unordered_set<Symbol> supportedOps = {
      "Select"_,       "Where"_,     "Project"_, "Join"_,  "Union"_,   "Except"_,  "Intersect"_,
      "Difference"_,   "Sort"_,      "SortBy"_,  "Group"_, "GroupBy"_, "Order"_,   "OrderBy"_,
      "Table"_,        "And"_,       "Or"_,      "Not"_,   "Equal"_,   "Greater"_, "Less"_,
      "GreaterEqual"_, "LessEqual"_, "Column"_,  "List"_,  "By"_,      "Top"_,     "Limit"_,
  };
  return supportedOps.find(op) != supportedOps.end();
}

bool isStaticValue(const Expression& expr) {
  return std::holds_alternative<int32_t>(expr) || std::holds_alternative<int64_t>(expr) ||
         std::holds_alternative<float>(expr) || std::holds_alternative<double>(expr) ||
         std::holds_alternative<bool>(expr) || std::holds_alternative<std::string>(expr) ||
         (std::holds_alternative<ComplexExpression>(expr) &&
          std::get<ComplexExpression>(expr).getHead() == "DateObject"_);
}

bool isInTransformationColumns(const std::unordered_map<Symbol, std::unordered_set<Symbol>>& transformationColumns,
                               const Symbol& symbol) {
  return transformationColumns.find(symbol) != transformationColumns.end();
}

// Places two bools in the result: one to show if extraction is possible.
// Another to signify if need to propagate to union, except, intersect
void verifyConditionExtraction(const Expression& inputExpression, const std::unordered_set<Symbol>& conditionColumns,
                               std::unordered_map<Symbol, std::unordered_set<Symbol>>& transformationColumns,
                               std::vector<bool>& result) {
  if (std::holds_alternative<ComplexExpression>(inputExpression)) {
    const auto& inputComplexExpression = std::get<ComplexExpression>(inputExpression);
    if (inputComplexExpression.getHead() == "As"_) {
      Symbol currentSymbol = Symbol("Symbol");
      for (const auto& arg : inputComplexExpression.getDynamicArguments()) {
        // Only a modified column can have complexexpression. If our symbol is there, then cannot extract
        if (std::holds_alternative<ComplexExpression>(arg)) {
          std::unordered_set<Symbol> subUsedSymbols = {};
          getUsedSymbolsFromExpressions(arg, subUsedSymbols, transformationColumns);
          for (const auto& symbol : conditionColumns) {
            if (subUsedSymbols.find(symbol) != subUsedSymbols.end()) {
              result[0] = false;
              return;
            }
          }
        }
      }
    } else if (inputComplexExpression.getHead() == "Union"_ || inputComplexExpression.getHead() == "Except"_ ||
               inputComplexExpression.getHead() == "Intersect"_ || inputComplexExpression.getHead() == "Difference"_) {
      result[1] = true;
      for (const auto& arg : inputComplexExpression.getDynamicArguments()) {
        verifyConditionExtraction(arg, conditionColumns, transformationColumns, result);
      }
    } else if (supportedOperator(inputComplexExpression.getHead())) {
      for (const auto& arg : inputComplexExpression.getDynamicArguments()) {
        verifyConditionExtraction(arg, conditionColumns, transformationColumns, result);
      }
    } else {
      std::unordered_set<Symbol> usedSymbols = {};
      getUsedSymbolsFromExpressions(inputExpression, usedSymbols, transformationColumns);
      for (const auto& symbol : conditionColumns) {
        // Used in some unknown function. So extraction is not possible
        if (usedSymbols.find(symbol) != usedSymbols.end()) {
          result[0] = false;
          return;
        }
      }
    }
  }
}

// Checks if the condition can be moved to the transformation query by
// checking if the columns in the condition are in the transformation query
// result set or are static values. Returns false if only static values are present
std::vector<bool> isConditionMoveable(const Expression& inputExpression, const ComplexExpression& condition,
                                      std::unordered_map<Symbol, std::unordered_set<Symbol>>& transformationColumns,
                                      std::unordered_set<Symbol>& usedSymbols) {
  if (condition.getHead() == "Greater"_ || condition.getHead() == "Equal"_) {
    auto firstColumns = getUsedTransformationColumns(condition.getDynamicArguments()[0], transformationColumns);
    auto secondColumns = getUsedTransformationColumns(condition.getDynamicArguments()[1], transformationColumns);
    // First value is whether extractable, second is whether inner Union, Except, Intersect are present
    std::vector<bool> result = {true, false};
    if (firstColumns.find(UNEXCTRACTABLE) != firstColumns.end() ||
        secondColumns.find(UNEXCTRACTABLE) != secondColumns.end() ||
        (firstColumns.size() == 0 && secondColumns.size() == 0)) {
      result[0] = false;
    }
    if (!result[0]) {
      return result;
    }
    firstColumns.insert(std::make_move_iterator(secondColumns.begin()), std::make_move_iterator(secondColumns.end()));
    // Condition might be moveable. Now have to validate by processing input
    // expression, see if it was not modified inside.
    verifyConditionExtraction(inputExpression, firstColumns, transformationColumns, result);
    if (!result[0]) {
      return result;
    }
    usedSymbols.insert(std::make_move_iterator(firstColumns.begin()), std::make_move_iterator(firstColumns.end()));
    usedSymbols.insert(std::make_move_iterator(secondColumns.begin()), std::make_move_iterator(secondColumns.end()));
    usedSymbols.erase(UNEXCTRACTABLE);
    return result;
  }
  return {false, false};
}

// Extracts used symbols from the expression
void getUsedSymbolsFromExpressions(const Expression& expr, std::unordered_set<Symbol>& usedSymbols) {
  std::unordered_map<Symbol, std::unordered_set<Symbol>> unused = {};
  getUsedSymbolsFromExpressions(expr, usedSymbols, unused);
}

// Extracts used symbols from the expression
// If transformationColumns are empty, then all symbols are added
// If transformationColumns are not empty, then only the symbols that are in the
// transformationColumns are added
void getUsedSymbolsFromExpressions(const Expression& expr, std::unordered_set<Symbol>& usedSymbols,
                                   std::unordered_map<Symbol, std::unordered_set<Symbol>>& transformationColumns) {
  if (std::holds_alternative<Symbol>(expr)) {
    Symbol symbol = std::get<Symbol>(expr);
    if (transformationColumns.size() == 0 || utilities::isInTransformationColumns(transformationColumns, symbol)) {
      usedSymbols.insert(symbol);
    }
  } else if (std::holds_alternative<ComplexExpression>(expr)) {
    const auto& complexExpr = std::get<ComplexExpression>(expr);
    for (const auto& arg : complexExpr.getDynamicArguments()) {
      getUsedSymbolsFromExpressions(arg, usedSymbols, transformationColumns);
    }
  }
}

// Returns a set of transformation columns that are present in the expression
// Adds UNEXCTRACTABLE to the set if the expression contains a column that is not in the
// transformation columns
std::unordered_set<Symbol> getUsedTransformationColumns(
    const Expression& expr, const std::unordered_map<Symbol, std::unordered_set<Symbol>>& transformationColumns) {
  if (std::holds_alternative<ComplexExpression>(expr)) {
    const auto& complexExpr = std::get<ComplexExpression>(expr);
    std::unordered_set<Symbol> usedSymbols = {};
    for (const auto& arg : complexExpr.getDynamicArguments()) {
      auto subUsedSymbols = getUsedTransformationColumns(arg, transformationColumns);
      usedSymbols.insert(std::make_move_iterator(subUsedSymbols.begin()), std::make_move_iterator(subUsedSymbols.end()));
    }
    return usedSymbols;
  } else if (std::holds_alternative<Symbol>(expr)) {
    Symbol symbol = std::get<Symbol>(expr);
    if (utilities::isInTransformationColumns(transformationColumns, symbol)) {
      return {std::get<Symbol>(expr.clone(boss::expressions::CloneReason::EXPRESSION_WRAPPING))};
    }
    return {UNEXCTRACTABLE};
  } else if (utilities::isStaticValue(expr)) {
    return {};
  }
  return {UNEXCTRACTABLE};
}

std::unordered_set<Symbol> getAllDependentSymbols(
    const std::unordered_map<Symbol, std::unordered_set<Symbol>>& transformationColumnsDependencies,
    const std::unordered_set<Symbol>& usedSymbols) {
  std::unordered_set<Symbol> dependentSymbols;

  for (const Symbol& symbol : usedSymbols) {
    dependentSymbols.insert(symbol);

    std::vector<Symbol> toProcess = {symbol};
    while (!toProcess.empty()) {
      Symbol& current = toProcess.back();
      toProcess.pop_back();

      auto it = transformationColumnsDependencies.find(current);
      if (it != transformationColumnsDependencies.end()) {
        for (const Symbol& dependency : it->second) {
          if (dependentSymbols.insert(dependency).second) {
            toProcess.push_back(dependency);
          }
        }
      }
    }
  }

  return std::move(dependentSymbols);
}

bool canMoveConditionThroughProjection(const ComplexExpression& projectionOperator,
                                       const ComplexExpression& extractedCondition) {
  std::unordered_set<Symbol> extractedConditionSymbols = {};
  for (const auto& arg : extractedCondition.getDynamicArguments()) {
    utilities::getUsedSymbolsFromExpressions(arg, extractedConditionSymbols);
  }
  return canMoveConditionThroughProjection(projectionOperator, extractedCondition, extractedConditionSymbols);
}

bool canMoveConditionThroughProjection(const ComplexExpression& projectionOperator,
                                       const ComplexExpression& extractedCondition,
                                       const std::unordered_set<Symbol>& extractedConditionSymbols) {
  const auto& projectionDynamics = projectionOperator.getDynamicArguments();
  const auto& projectionAsFunction = std::get<ComplexExpression>(projectionDynamics[1]);
  bool resultColumn = true;  // Result column is every the output name column
  bool isSymbolUsedInCondition = false;
  Symbol currentSymbol = Symbol("Symbol");
  for (const auto& arg : projectionAsFunction.getDynamicArguments()) {
    if (resultColumn) {
      currentSymbol = std::get<Symbol>(arg);
      if (extractedConditionSymbols.find(currentSymbol) != extractedConditionSymbols.end()) {
        isSymbolUsedInCondition = true;
      } else {
        isSymbolUsedInCondition = false;
      }
      resultColumn = false;
    } else {
      if (!std::holds_alternative<Symbol>(arg)) {
        std::unordered_set<Symbol> usedSymbols = {};
        utilities::getUsedSymbolsFromExpressions(arg, usedSymbols);
        for (const auto& symbol : extractedConditionSymbols) {
          if (usedSymbols.find(symbol) != usedSymbols.end()) {
            // The logic is the following: If the extracted condition symbol is used to generate some results, then
            // altering (filtering) the symbol  will alter the results. Therefore, can't be moved
            return false;
          }
        }
      }
      if (isSymbolUsedInCondition) {
        if (!utilities::isOperationReversible(arg)) {
          return false;
        }
      }
      resultColumn = true;
    }
  }
  return true;
}

// Part of future work: Extend with reversing the condition when propagate through simple transformations
// E.g. when go through "Plus"(1, "A"), update the condition to subtract 1 from "A"
// Checks if the operation is reversible
bool isOperationReversible(const Expression& expr) {
  if (std::holds_alternative<Symbol>(expr)) {
    return true;
  } else {
    return false;
  }
}

void buildColumnDependencies(const ComplexExpression& expr,
                             std::unordered_map<Symbol, std::unordered_set<Symbol>>& transformationColumnsDependencies,
                             std::unordered_set<Symbol>& untouchableColumns) {
  for (const auto& arg : expr.getDynamicArguments()) {
    if (std::holds_alternative<ComplexExpression>(arg)) {
      const auto& subExpr = std::get<ComplexExpression>(arg);
      if (subExpr.getHead() == "As"_) {
        Symbol currentSymbol = Symbol("Symbol");
        for (const auto& arg : subExpr.getDynamicArguments()) {
          if (std::holds_alternative<Symbol>(arg)) {
            // TODO: Might be wastefull to copy if not used
            currentSymbol = std::get<Symbol>(arg);
            if (transformationColumnsDependencies.find(currentSymbol) == transformationColumnsDependencies.end()) {
              transformationColumnsDependencies[currentSymbol] = {};
            }
          } else {
            std::unordered_set<Symbol> dependentOnSymbols = {};
            utilities::getUsedSymbolsFromExpressions(arg, dependentOnSymbols);
            transformationColumnsDependencies[currentSymbol].insert(std::make_move_iterator(dependentOnSymbols.begin()),
                                                                    std::make_move_iterator(dependentOnSymbols.end()));
          }
        }
      } else if (subExpr.getHead() == "Where"_) {
        std::unordered_set<Symbol> usedSymbols = {};
        utilities::getUsedSymbolsFromExpressions(subExpr.getDynamicArguments()[0], usedSymbols);
        for (const auto& symbol : usedSymbols) {
          untouchableColumns.insert(symbol);
          if (transformationColumnsDependencies.find(symbol) == transformationColumnsDependencies.end()) {
            transformationColumnsDependencies[symbol] = {};
          }
        }
      } else {
        buildColumnDependencies(subExpr, transformationColumnsDependencies, untouchableColumns);
      }
    } else if (std::holds_alternative<Symbol>(arg)) {
      Symbol column = std::get<Symbol>(arg);
      if (transformationColumnsDependencies.find(column) == transformationColumnsDependencies.end()) {
        transformationColumnsDependencies[column] = {};
      }
    }
  }
}

Expression wrapOperatorWithSelect(Expression&& expr, ComplexExpression&& condition) {
  auto newWhere = boss::ComplexExpression("Where"_, {}, boss::ExpressionArguments(std::move(condition)), {});
  auto newSelect =
      boss::ComplexExpression("Select"_, {}, boss::ExpressionArguments(std::move(expr), std::move(newWhere)), {});
  return boss::Expression(std::move(newSelect));
}

// Merges two consecutive SELECT operators
// If there aren't two consecutive SELECT operators, then the original expression is returned
ComplexExpression mergeConsecutiveSelectOperators(ComplexExpression&& outerSelect) {
  if (outerSelect.getHead() != "Select"_) {
    return std::move(outerSelect);
  }
  auto [outerHead, outerStatics, outerDynamics, outerSpans] = std::move(outerSelect).decompose();
  if (!std::holds_alternative<ComplexExpression>(outerDynamics[0])) {
    return boss::ComplexExpression(std::move(outerHead), std::move(outerStatics), std::move(outerDynamics),
                                   std::move(outerSpans));
  }
  auto innerSelect = std::get<ComplexExpression>(std::move(outerDynamics[0]));
  if (innerSelect.getHead() != "Select"_) {
    outerSelect = boss::ComplexExpression(std::move(outerHead), std::move(outerStatics),
                                          boss::ExpressionArguments(std::move(innerSelect), std::move(outerDynamics[1])),
                                          std::move(outerSpans));
    return std::move(outerSelect);
  }

  auto [innerHead, innerStatics, innerDynamics, innerSpans] = std::move(innerSelect).decompose();
  auto outerWhere = std::get<ComplexExpression>(std::move(outerDynamics[1]));
  auto innerWhere = std::get<ComplexExpression>(std::move(innerDynamics[1]));
  auto [innerWhereHead, innerWhereStatics, innerWhereDynamics, innerWhereSpans] = std::move(innerWhere).decompose();
  auto innerWhereCondition = std::get<ComplexExpression>(std::move(innerWhereDynamics[0]));
  auto newOuterWhere = addConditionToWhereOperator(std::move(outerWhere), std::move(innerWhereCondition));
  auto newOuterSelect = boss::ComplexExpression(
      std::move(outerHead), std::move(outerStatics),
      boss::ExpressionArguments(std::move(innerDynamics[0]), std::move(newOuterWhere)), std::move(outerSpans));
  return std::move(newOuterSelect);
}

ComplexExpression addConditionToWhereOperator(ComplexExpression&& whereOperator, ComplexExpression&& condition) {
  auto [whereHead, whereStatics, whereDynamics, whereSpans] = std::move(whereOperator).decompose();
  auto whereDynamicsExpression = std::get<ComplexExpression>(std::move(whereDynamics[0]));
  if (whereDynamicsExpression.getHead() == "And"_) {
    auto [andHead, andStatics, andDynamics, andSpans] = std::move(whereDynamicsExpression).decompose();
    if (condition.getHead() == "And"_) {
      auto [conditionHead, conditionStatics, conditionDynamics, conditionSpans] = std::move(condition).decompose();
      for (auto& subCondition : conditionDynamics) {
        andDynamics.emplace_back(std::move(subCondition));
      }
    } else {
      andDynamics.emplace_back(std::move(condition));
    }
    auto reconstructedWhereDynamicsExpression =
        boss::ComplexExpression(std::move(andHead), std::move(andStatics), std::move(andDynamics), std::move(andSpans));
    return boss::ComplexExpression(std::move(whereHead), std::move(whereStatics),
                                   boss::ExpressionArguments(std::move(reconstructedWhereDynamicsExpression)),
                                   std::move(whereSpans));
  } else {
    if (condition.getHead() == "And"_) {
      auto [conditionHead, conditionStatics, conditionDynamics, conditionSpans] = std::move(condition).decompose();
      conditionDynamics.emplace_back(std::move(whereDynamicsExpression));
      auto newWhereDynamics = boss::ComplexExpression(std::move(conditionHead), std::move(conditionStatics),
                                                      std::move(conditionDynamics), std::move(conditionSpans));
      return boss::ComplexExpression(std::move(whereHead), std::move(whereStatics),
                                     boss::ExpressionArguments(std::move(newWhereDynamics)), std::move(whereSpans));
    } else {
      auto newWhereDynamics = boss::ComplexExpression(
          "And"_, {}, boss::ExpressionArguments(std::move(whereDynamicsExpression), std::move(condition)), {});
      return boss::ComplexExpression(std::move(whereHead), std::move(whereStatics),
                                     boss::ExpressionArguments(std::move(newWhereDynamics)), std::move(whereSpans));
    }
  }
}
}  // namespace boss::engines::LazyTransformation::utilities
