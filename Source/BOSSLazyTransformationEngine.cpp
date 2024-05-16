#include "BOSSLazyTransformationEngine.hpp"

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

using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;

using boss::Expression;

namespace boss::engines::LazyTransformation {

namespace utilities {

bool isCardinalityReducingOperator(const Symbol& op) {
  static const std::unordered_set<Symbol> cardinalityReducingOps = {
      "Select"_, "Project"_  //, "Top"_, //"Group"_
  };
  return cardinalityReducingOps.find(op) != cardinalityReducingOps.end();
}

bool isStaticValue(const Expression& expr) {
  return std::holds_alternative<int32_t>(expr) || std::holds_alternative<int64_t>(expr) ||
         std::holds_alternative<float>(expr) || std::holds_alternative<double>(expr) ||
         std::holds_alternative<bool>(expr) || std::holds_alternative<std::string>(expr) ||
         (std::holds_alternative<ComplexExpression>(expr) &&
          std::get<ComplexExpression>(expr).getHead() == "DateObject"_);
}

bool isInTransformationColumns(const std::unordered_set<Symbol> transformationColumns, const Symbol& symbol) {
  return transformationColumns.find(symbol) != transformationColumns.end();
}

// Checks if the condition can be moved to the transformation query by
// checking if the columns in the condition are in the transformation query
// result set or are static values. Returns false if only static values are present
bool isConditionMoveable(const ComplexExpression& condition, std::unordered_set<Symbol> transformationColumns) {
  if (condition.getHead() == "Greater"_ || condition.getHead() == "Equal"_) {
    auto firstColumns = getUsedTransformationColumns(condition.getDynamicArguments()[0], transformationColumns);
    auto secondColumns = getUsedTransformationColumns(condition.getDynamicArguments()[1], transformationColumns);
    if (firstColumns.find(UNEXCTRACTABLE) != firstColumns.end() ||
        secondColumns.find(UNEXCTRACTABLE) != secondColumns.end() ||
        (firstColumns.size() == 0 && secondColumns.size() == 0)) {
      return false;
    }
    return true;
  }
  return false;
}

// Extracts used symbols from the expression
void getUsedSymbolsFromExpressions(const Expression& expr, std::unordered_set<Symbol>& usedSymbols) {
  std::unordered_set<Symbol> unusedColumns = {};
  getUsedSymbolsFromExpressions(expr, usedSymbols, unusedColumns);
}

// Extracts used symbols from the expression
// If transformationColumns are empty, then all symbols are added
// If transformationColumns are not empty, then only the symbols that are in the transformationColumns are added
void getUsedSymbolsFromExpressions(const Expression& expr, std::unordered_set<Symbol>& usedSymbols,
                                   std::unordered_set<Symbol>& transformationColumns) {
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
// Adds UNEXCTRACTABLE to the set if the expression contains a column that is not in the transformation columns
std::unordered_set<Symbol> getUsedTransformationColumns(const Expression& expr,
                                                        const std::unordered_set<Symbol>& transformationColumns) {
  if (std::holds_alternative<ComplexExpression>(expr)) {
    const auto& complexExpr = std::get<ComplexExpression>(expr);
    std::unordered_set<Symbol> usedSymbols = {};
    for (const auto& arg : complexExpr.getDynamicArguments()) {
      auto subUsedSymbols = getUsedTransformationColumns(arg, transformationColumns);
      usedSymbols.insert(subUsedSymbols.begin(), subUsedSymbols.end());
    }
    return usedSymbols;
  } else if (std::holds_alternative<Symbol>(expr)) {
    Symbol symbol = std::get<Symbol>(expr);
    if (utilities::isInTransformationColumns(transformationColumns, symbol)) {
      return {std::get<Symbol>(expr.clone(expressions::CloneReason::EXPRESSION_WRAPPING))};
    }
    return {UNEXCTRACTABLE};
  } else if (utilities::isStaticValue(expr)) {
    return {};
  }
  return {UNEXCTRACTABLE};
}

bool canMoveConditionThroughProjection(const ComplexExpression& projectionOperator,
                                       const ComplexExpression& extractedCondition) {
  const auto& projectionDynamics = projectionOperator.getDynamicArguments();
  const auto& projectionAsFunction = std::get<ComplexExpression>(projectionDynamics[1]);
  std::unordered_set<Symbol> usedSymbols = {};
  for (const auto& arg : extractedCondition.getDynamicArguments()) {
    utilities::getUsedSymbolsFromExpressions(arg, usedSymbols);
  }
  bool transformationColumn = false;
  bool isSymbolUsedInCondition = false;
  Symbol currentSymbol = Symbol("Symbol");

  for (const auto& arg : projectionAsFunction.getDynamicArguments()) {
    if (!transformationColumn) {
      currentSymbol = std::get<Symbol>(arg);
      transformationColumn = true;
      if (usedSymbols.find(currentSymbol) != usedSymbols.end()) {
        isSymbolUsedInCondition = true;
      }
    } else {
      if (isSymbolUsedInCondition) {
        if (!utilities::isOperationReversible(arg)) {
          return false;
        }
      }
      transformationColumn = false;
    }
  }
  return true;
}

// Checks if the operation is reversible
bool isOperationReversible(const Expression& expr) {
  if (std::holds_alternative<Symbol>(expr)) {
    return true;
  } else if (std::holds_alternative<ComplexExpression>(expr)) {
    // TODO: Think how to do it properly
    if (std::get<ComplexExpression>(expr).getHead() == "Minus"_) {
      return false;
    } else if (std::get<ComplexExpression>(expr).getHead() == "Plus"_) {
      return false;
    } else if (std::get<ComplexExpression>(expr).getHead() == "Multiply"_) {
      return false;
    } else {
      return false;
    }
  } else {
    return false;
  }
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
    outerSelect = boss::ComplexExpression(
        std::move(outerHead), std::move(outerStatics),
        boss::ExpressionArguments(std::move(innerSelect), std::move(outerDynamics[1])), std::move(outerSpans));
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
}  // namespace utilities

Engine::Engine(ComplexExpression&& transformationQuery)
    : transformationQuery(std::move(transformationQuery)), transformationColumns(extractTransformationColumns()) {}

// ! It is used to know which columns from the input query we can extract back into the transformation
// ? Maybe can use some other query scan, so we don't have to force to end the transformation with the Project operator
std::unordered_set<Symbol> Engine::extractTransformationColumns() {
  std::unordered_set<Symbol> transformationQueryColumns = {};
  if (transformationQuery.getHead() == "Project"_) {
    for (const auto& arg : transformationQuery.getDynamicArguments()) {
      if (std::holds_alternative<ComplexExpression>(arg)) {
        const auto& subExpr = std::get<ComplexExpression>(arg);
        if (subExpr.getHead() == "As"_) {
          for (const auto& arg : subExpr.getDynamicArguments()) {
            if (std::holds_alternative<Symbol>(arg)) {
              transformationQueryColumns.insert(std::get<Symbol>(arg));
            }
          }
        }
      }
    }
    return transformationQueryColumns;
  } else {
    throw std::runtime_error(
        "Transformation query should end with a Project operator in order to extract available columns");
  }
}

// ---------------------------- SELECTION RELATED OPERATIONS START ----------------------------
// TODO: Add subprocessing for the nested expressions
Expression Engine::extractOperatorsFromSelect(ComplexExpression&& expr,
                                              std::vector<ComplexExpression>& conditionsToMove,
                                              std::unordered_set<Symbol>& projectionColumns) {
  // decomposes the SELECT expression
  auto [selectHead, _, selectDynamics, unused1] = std::move(expr).decompose();
  // gets the WHERE expression
  auto&& whereExpression = std::get<ComplexExpression>(std::move(selectDynamics[1]));
  // decomposes the WHERE expression
  auto [whereHead, unused2, whereDynamics, unused3] = std::move(whereExpression).decompose();
  // gets the WHERE condition expression
  auto&& conditionExpression = std::get<ComplexExpression>(std::move(whereDynamics[0]));

  auto processedInput = processExpression(std::move(selectDynamics[0]), projectionColumns);

  // If the condition is a simple single condition
  if (conditionExpression.getHead() == "Greater"_ || conditionExpression.getHead() == "Equal"_) {
    if (utilities::isConditionMoveable(conditionExpression, transformationColumns)) {
      // If the single condition is extractable, then we can remove the whole SELECT operator and return its input
      conditionsToMove.emplace_back(std::move(conditionExpression));
      return boss::Expression(std::move(processedInput));
    }
  } else if (conditionExpression.getHead() == "And"_) {  // TODO: Add support for OR operator
    // You can always extract from the AND operator, unless AND is inside of the OR operator
    // If you have an OR operator, then you can extract the condition if the condition on that column is present
    // in all OR branches
    auto [andHead, unused4, andDynamics, unused5] = std::move(conditionExpression).decompose();
    boss::ExpressionArguments remainingSubconditions = {};
    // If the condition is a complex condition, we need to check each subcondition
    for (auto& andSubcondition : andDynamics) {
      auto& subConditionExpr = std::get<ComplexExpression>(andSubcondition);
      if (utilities::isConditionMoveable(subConditionExpr, transformationColumns)) {
        conditionsToMove.emplace_back(std::move(subConditionExpr));
      } else {
        remainingSubconditions.emplace_back(std::move(subConditionExpr));
      }
    }

    if (remainingSubconditions.size() == 0) {
      // If all subconditions are extractable, then we can remove the whole SELECT operator and return its input
      return boss::Expression(std::move(processedInput));
    } else {
      // If there are remaining subconditions, we need to create a new expression with them
      if (remainingSubconditions.size() == 1) {
        // If size is one, we don't need to recreate the whole AND operator
        auto newWhere = boss::ComplexExpression(std::move(whereHead), {},
                                                boss::ExpressionArguments(std::move(remainingSubconditions[0])), {});
        auto newSelect = boss::ComplexExpression(
            std::move(selectHead), {}, boss::ExpressionArguments(std::move(processedInput), std::move(newWhere)), {});
        return boss::Expression(std::move(newSelect));
      } else {
        auto newAnd = boss::ComplexExpression(std::move(andHead), {}, {std::move(remainingSubconditions)}, {});
        auto newWhere =
            boss::ComplexExpression(std::move(whereHead), {}, boss::ExpressionArguments(std::move(newAnd)), {});
        auto newSelect = boss::ComplexExpression(
            std::move(selectHead), {}, boss::ExpressionArguments(std::move(processedInput), std::move(newWhere)), {});
        return boss::Expression(std::move(newSelect));
      }
    }
  }
  // Compose and return the original expression in case if the condition is not extractable
  auto oldWhere =
      boss::ComplexExpression(std::move(whereHead), {}, boss::ExpressionArguments(std::move(conditionExpression)), {});
  auto oldSelect = boss::ComplexExpression(
      std::move(selectHead), {}, boss::ExpressionArguments(std::move(processedInput), std::move(oldWhere)), {});
  return boss::Expression(std::move(oldSelect));
}
// ---------------------------- SELECTION RELATED OPERATIONS START ----------------------------

// ---------------------------- EXPRESSION PROPAGATION RELATED OPERATIONS START ----------------------------

ComplexExpression moveExctractedSelectExpressionToTransformation(ComplexExpression&& transformingExpression,
                                                                 ComplexExpression&& extractedExpression) {
  if (transformingExpression.getHead() == "Select"_) {
    auto [head, statics, dynamics, spans] = std::move(transformingExpression).decompose();
    auto selectInput = std::get<ComplexExpression>(std::move(dynamics[0]));
    auto newSelectInput =
        moveExctractedSelectExpressionToTransformation(std::move(selectInput), std::move(extractedExpression));
    auto newTransformingExpression = boss::ComplexExpression(std::move(head), std::move(statics),
                                                             boss::ExpressionArguments(std::move(newSelectInput)), {});
    return std::move(utilities::mergeConsecutiveSelectOperators(std::move(newTransformingExpression)));
    // For the Table or Column we can't propagate further, so add it to the WHERE operator
  } else if (transformingExpression.getHead() == "Project"_) {
    if (utilities::canMoveConditionThroughProjection(transformingExpression, extractedExpression)) {
      auto [head, statics, dynamics, spans] = std::move(transformingExpression).decompose();
      auto projectInput = std::get<ComplexExpression>(std::move(dynamics[0]));
      // TODO: Need to modify the projection function and the extractedExpression
      auto newProjectInput =
          moveExctractedSelectExpressionToTransformation(std::move(projectInput), std::move(extractedExpression));
      return boss::ComplexExpression(std::move(head), std::move(statics),
                                     boss::ExpressionArguments(std::move(newProjectInput), std::move(dynamics[1])),
                                     std::move(spans));
    }
  }
  // TODO: Add support for other operators: Join, Group, etc.
  // Encapsulate the transformingExpression into the new select
  auto newWhere = boss::ComplexExpression("Where"_, {}, boss::ExpressionArguments(std::move(extractedExpression)), {});
  auto newSelect = boss::ComplexExpression(
      "Select"_, {}, boss::ExpressionArguments(std::move(transformingExpression), std::move(newWhere)), {});
  return std::move(newSelect);
}

// ---------------------------- EXPRESSION PROPAGATION RELATED OPERATIONS END ----------------------------

// TODO: Thinkg if TOP operator should be implemented
void Engine::extractOperatorsFromTop(const ComplexExpression& expr) {
  // TODO: Implement the logic for moving operators to the transformation
}

Expression Engine::processExpression(Expression&& inputExpr, std::unordered_set<Symbol>& projectionColumns) {
  return std::visit(
      boss::utilities::overload(
          [this, &projectionColumns](ComplexExpression&& complexExpr) -> Expression {
            if (complexExpr.getHead() == "Transformation"_) {
              return boss::Expression(std::move(complexExpr));
            } else if (complexExpr.getHead() == "Select"_) {
              std::vector<ComplexExpression> extractedExpressions = {};
              auto expression =
                  extractOperatorsFromSelect(std::move(complexExpr), extractedExpressions, projectionColumns);
              for (auto& extractedExpr : extractedExpressions) {
                transformationQuery = moveExctractedSelectExpressionToTransformation(std::move(transformationQuery),
                                                                                     std::move(extractedExpr));
              }
              return std::move(expression);
            } else if (complexExpr.getHead() == "Project"_) {
              // TODO: Think if the project is needed, or can we run getUsedTransformationColumns on the expression
              auto [head, _, dynamics, unused] = std::move(complexExpr).decompose();
              // Process Project's input expression
              auto& projectionInputExpr = std::get<ComplexExpression>(dynamics[0]);
              auto updatedInput =
                  processExpression(std::move(boss::Expression(std::move(projectionInputExpr))), projectionColumns);

              // Extract used symbols from the projection function
              auto& projectionAsExpr = std::get<ComplexExpression>(dynamics[1]);
              for (const auto& arg : projectionAsExpr.getDynamicArguments()) {
                utilities::getUsedSymbolsFromExpressions(arg, projectionColumns);
              }
              // Compose and return the original expression
              boss::ExpressionArguments&& remainingSubconditions = {};
              remainingSubconditions.emplace_back(std::move(updatedInput));
              remainingSubconditions.emplace_back(boss::Expression(std::move(projectionAsExpr)));
              ComplexExpression resultExpr = boss::ComplexExpression(head, {}, {std::move(remainingSubconditions)}, {});
              return boss::Expression(std::move(resultExpr));
            }

            auto [head, statics, dynamics, spans] = std::move(complexExpr).decompose();
            // Recursively process sub-expressions
            std::transform(std::make_move_iterator(dynamics.begin()), std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [&](auto&& subExpr) {
                             return processExpression(std::forward<decltype(subExpr)>(subExpr), projectionColumns);
                           });
            return boss::ComplexExpression(head, std::move(statics), std::move(dynamics), std::move(spans));
          },
          [this](Symbol&& symbol) -> Expression { return boss::Expression(std::move(symbol)); },
          [](auto&& otherExpr) -> Expression { return std::forward<decltype(otherExpr)>(otherExpr); }),
      std::move(inputExpr));
}

Expression Engine::evaluate(Expression&& expr) {
  return std::visit(boss::utilities::overload(
                        [this](ComplexExpression&& complexExpr) -> Expression {
                          std::unordered_set<Symbol> usedSymbols = {};

                          Expression result = processExpression(std::move(complexExpr), usedSymbols);
                          // TODO: Find "Transform" operator and replace it with the transformationQuery
                          return result;
                        },
                        [this](Symbol&& symbol) -> Expression { return std::move(symbol); },
                        [](auto&& otherExpr) -> Expression { return std::forward<decltype(otherExpr)>(otherExpr); }),
                    std::move(expr));
}

static std::string expressionToString(Expression&& expr) {
  return std::visit(boss::utilities::overload(
                        [](ComplexExpression& complexExpr) -> std::string {
                          std::string result = complexExpr.getHead().getName() + "(";
                          auto [head, statics, dynamics, spans] = std::move(complexExpr).decompose();
                          for (auto& arg : dynamics) {
                            result += expressionToString(boss::Expression(std::move(arg)));
                            result += ", ";
                          }
                          result.pop_back();
                          result.pop_back();
                          result += ")";
                          return result;
                        },
                        [](Symbol& symbol) -> std::string { return symbol.getName(); },
                        [](auto& otherExpr) -> std::string { return "I DONT KNOW WHAT IT IS"; }),
                    expr);
}

}  // namespace boss::engines::LazyTransformation

// void Engine::setTransformation() {
//   SELECT
//     id,
//     MAX(CASE WHEN columnName = 'columnName1' THEN value END) AS columnName1,
//     MAX(CASE WHEN columnName = 'columnName2' THEN value END) AS columnName2
// FROM
//     source_data
// GROUP BY
//     id;
// }

// TODO: Check if this is the correct way to handle the engine
static auto& enginePtr(bool initialise = true) {
  // TODO: Engine need to be initialised with the transformation query
  static auto engine = std::unique_ptr<boss::engines::LazyTransformation::Engine>();
  if (!engine && initialise) {
    //  TODO: Uncomment this line when the transformation query is ready
    // engine.reset(new boss::engines::LazyTransformation::Engine());
  }
  return engine;
}

extern "C" BOSSExpression* evaluate(BOSSExpression* e) {
  static std::mutex m;
  std::lock_guard lock(m);
  auto* r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
  return r;
};

extern "C" void reset() { enginePtr(false).reset(nullptr); }
