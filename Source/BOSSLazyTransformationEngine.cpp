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

#include "Utilities.hpp"

using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;

using boss::Expression;

namespace boss::engines::LazyTransformation {

// ---------------------------- EXPRESSION EXTRACTION RELATED OPERATIONS START ----------------------------
// TODO: Add subprocessing for the nested expressions
Expression Engine::extractOperatorsFromSelect(
    ComplexExpression&& expr, std::vector<ComplexExpression>& conditionsToMove,
    std::unordered_map<Symbol, std::unordered_set<Symbol>>& transformationColumnsDependencies,
    std::unordered_set<Symbol>& usedSymbols) {
  // decomposes the SELECT expression
  auto [selectHead, _, selectDynamics, unused1] = std::move(expr).decompose();
  // gets the WHERE expression
  auto&& whereExpression = std::get<ComplexExpression>(std::move(selectDynamics[1]));
  // decomposes the WHERE expression
  auto [whereHead, unused2, whereDynamics, unused3] = std::move(whereExpression).decompose();
  // gets the WHERE condition expression
  auto&& conditionExpression = std::get<ComplexExpression>(std::move(whereDynamics[0]));

  auto processedInput = processExpression(std::move(selectDynamics[0]), transformationColumnsDependencies, usedSymbols);

  // If the condition is a simple single condition
  if (conditionExpression.getHead() == "Greater"_ || conditionExpression.getHead() == "Equal"_) {
    if (utilities::isConditionMoveable(conditionExpression, transformationColumnsDependencies, usedSymbols)) {
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
      auto subConditionExpr = std::get<ComplexExpression>(std::move(andSubcondition));
      if (utilities::isConditionMoveable(subConditionExpr, transformationColumnsDependencies, usedSymbols)) {
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
        auto newWhere = boss::ComplexExpression(std::move(whereHead), {}, boss::ExpressionArguments(std::move(newAnd)), {});
        auto newSelect = boss::ComplexExpression(
            std::move(selectHead), {}, boss::ExpressionArguments(std::move(processedInput), std::move(newWhere)), {});
        return boss::Expression(std::move(newSelect));
      }
    }
  }
  // Compose and return the original expression in case if the condition is not extractable
  auto oldWhere =
      boss::ComplexExpression(std::move(whereHead), {}, boss::ExpressionArguments(std::move(conditionExpression)), {});
  auto oldSelect = boss::ComplexExpression(std::move(selectHead), {},
                                           boss::ExpressionArguments(std::move(processedInput), std::move(oldWhere)), {});
  return boss::Expression(std::move(oldSelect));
}
// ---------------------------- EXPRESSION EXTRACTION RELATED OPERATIONS START ----------------------------

// ---------------------------- EXPRESSION PROPAGATION RELATED OPERATIONS START ----------------------------

ComplexExpression moveExctractedSelectExpressionToTransformation(ComplexExpression&& transformingExpression,
                                                                 ComplexExpression&& extractedExpression,
                                                                 const std::unordered_set<Symbol>& extractedExprSymbols) {
  if (transformingExpression.getHead() == "Select"_) {
    // Push through the SELECT operator
    auto [head, statics, dynamics, spans] = std::move(transformingExpression).decompose();
    auto selectInput = std::get<ComplexExpression>(std::move(dynamics[0]));
    auto newSelectInput = moveExctractedSelectExpressionToTransformation(
        std::move(selectInput), std::move(extractedExpression), extractedExprSymbols);
    auto newTransformingExpression =
        boss::ComplexExpression(std::move(head), std::move(statics),
                                boss::ExpressionArguments(std::move(newSelectInput), std::move(dynamics[1])), {});
    return std::move(utilities::mergeConsecutiveSelectOperators(std::move(newTransformingExpression)));
    // For the Table or Column we can't propagate further, so add it to the WHERE operator
  } else if (transformingExpression.getHead() == "Project"_) {
    // Push through the PROJECT operator if the condition can be moved through the projection
    if (utilities::canMoveConditionThroughProjection(transformingExpression, extractedExpression, extractedExprSymbols)) {
      auto [head, statics, dynamics, spans] = std::move(transformingExpression).decompose();
      auto projectInput = std::get<ComplexExpression>(std::move(dynamics[0]));
      // TODO: Need to modify the projection function and the extractedExpression
      auto newProjectInput = moveExctractedSelectExpressionToTransformation(
          std::move(projectInput), std::move(extractedExpression), extractedExprSymbols);
      return boss::ComplexExpression(std::move(head), std::move(statics),
                                     boss::ExpressionArguments(std::move(newProjectInput), std::move(dynamics[1])),
                                     std::move(spans));
    }
  } else if (transformingExpression.getHead() == "Sort"_ || transformingExpression.getHead() == "SortBy"_ ||
             transformingExpression.getHead() == "Order"_ || transformingExpression.getHead() == "OrderBy"_) {
    // Can safely push through Sort and Order as filtering rows doesn't change the order
    auto [head, statics, dynamics, spans] = std::move(transformingExpression).decompose();
    auto expressionInput = std::get<ComplexExpression>(std::move(dynamics[0]));
    auto newExpressionInput = moveExctractedSelectExpressionToTransformation(
        std::move(expressionInput), std::move(extractedExpression), extractedExprSymbols);
    return boss::ComplexExpression(std::move(head), std::move(statics),
                                   boss::ExpressionArguments(std::move(newExpressionInput), std::move(dynamics[1])),
                                   std::move(spans));

  } else if (transformingExpression.getHead() == "Group"_ || transformingExpression.getHead() == "GroupBy"_) {
    // Can push through Group only when the condition is on the grouped column, otherwise grouping result will change
    const auto& constDynamics = transformingExpression.getDynamicArguments();
    const auto& groupByExpression = constDynamics[1];
    // Sometimes Group operator only has the aggregated columns, missing the By. Can't push through it in that case
    if (std::get<ComplexExpression>(groupByExpression).getHead() == "By"_) {
      std::unordered_set<Symbol> groupingColumns = {};
      utilities::getUsedSymbolsFromExpressions(groupByExpression, groupingColumns);
      bool canPushThrough = true;
      for (const auto& symbol : extractedExprSymbols) {
        if (groupingColumns.find(symbol) == groupingColumns.end()) {
          canPushThrough = false;
          break;
        }
      }
      if (canPushThrough) {
        auto [head, statics, dynamics, spans] = std::move(transformingExpression).decompose();
        auto expressionInput = std::get<ComplexExpression>(std::move(dynamics[0]));
        auto newExpressionInput = moveExctractedSelectExpressionToTransformation(
            std::move(expressionInput), std::move(extractedExpression), extractedExprSymbols);
        ExpressionArguments newGroupByArguments = {};
        newGroupByArguments.emplace_back(std::move(newExpressionInput));
        newGroupByArguments.emplace_back(std::move(dynamics[1]));
        if (dynamics.size() == 3) {
          newGroupByArguments.emplace_back(std::move(dynamics[2]));
        }
        return boss::ComplexExpression(std::move(head), std::move(statics), std::move(newGroupByArguments),
                                       std::move(spans));
      }
    }
    // If cannot move through the Group operator, drop to the base case to wrap it with the SELECT operator
  } else if (transformingExpression.getHead() == "Join"_) {
    // Can push through if the condition is only on one of the Join inputs
    const auto& constDynamics = transformingExpression.getDynamicArguments();

    const auto& joinInput1 = constDynamics[0];
    std::unordered_set<Symbol> joinInput1Columns = {};
    utilities::getUsedSymbolsFromExpressions(joinInput1, joinInput1Columns);

    const auto& joinInput2 = constDynamics[1];
    std::unordered_set<Symbol> joinInput2Columns = {};
    utilities::getUsedSymbolsFromExpressions(joinInput2, joinInput2Columns);

    bool canPushThrough1 = true;
    bool canPushThrough2 = true;
    bool isInFirstInput = false;
    bool isInSecondInput = false;

    // Check if the condition is only on the first input of the Join operator
    for (const auto& symbol : extractedExprSymbols) {
      isInFirstInput = joinInput1Columns.find(symbol) != joinInput1Columns.end();
      isInSecondInput = joinInput2Columns.find(symbol) != joinInput2Columns.end();

      // If simultaneously in both inputs, can't push through
      if (isInFirstInput && isInSecondInput) {
        canPushThrough1 = false;
        canPushThrough2 = false;
        break;
      }
      if (!isInFirstInput) {
        canPushThrough1 = false;
      }
      if (!isInSecondInput) {
        canPushThrough2 = false;
      }
      if (!canPushThrough1 && !canPushThrough2) {
        break;
      }
    }

    // Check that they are not both true or both false
    if (canPushThrough1 != canPushThrough2) {
      auto [head, statics, dynamics, spans] = std::move(transformingExpression).decompose();
      if (canPushThrough1) {
        auto newJoinInput1 =
            moveExctractedSelectExpressionToTransformation(std::move(std::get<ComplexExpression>(std::move(dynamics[0]))),
                                                           std::move(extractedExpression), extractedExprSymbols);
        return boss::ComplexExpression(
            std::move(head), std::move(statics),
            boss::ExpressionArguments(std::move(newJoinInput1), std::move(dynamics[1]), std::move(dynamics[2])),
            std::move(spans));
      } else if (canPushThrough2) {
        auto newJoinInput2 =
            moveExctractedSelectExpressionToTransformation(std::move(std::get<ComplexExpression>(std::move(dynamics[1]))),
                                                           std::move(extractedExpression), extractedExprSymbols);
        return boss::ComplexExpression(
            std::move(head), std::move(statics),
            boss::ExpressionArguments(std::move(dynamics[0]), std::move(newJoinInput2), std::move(dynamics[2])),
            std::move(spans));
      }
    }
    // If cannot move through the Join operator, drop to the base case to wrap it with the SELECT operator
  }
  // Encapsulate the transformingExpression into the new select
  auto newWhere = boss::ComplexExpression("Where"_, {}, boss::ExpressionArguments(std::move(extractedExpression)), {});
  auto newSelect = boss::ComplexExpression(
      "Select"_, {}, boss::ExpressionArguments(std::move(transformingExpression), std::move(newWhere)), {});
  return std::move(newSelect);
}

ComplexExpression removeUnusedTransformationColumns(ComplexExpression&& transformationQuery,
                                                    const std::unordered_set<Symbol>& usedSymbols,
                                                    const std::unordered_set<Symbol>& untouchableColumns) {
  if (transformationQuery.getHead() == "Project"_) {
    auto [head, statics, dynamics, spans] = std::move(transformationQuery).decompose();
    auto projectInput = std::get<ComplexExpression>(std::move(dynamics[0]));
    auto newProjectInput = removeUnusedTransformationColumns(std::move(projectInput), usedSymbols, untouchableColumns);
    auto projectionAsExpr = std::get<ComplexExpression>(std::move(dynamics[1]));
    auto [asExprHead, asExprStatics, asExprDynamics, asExprSpans] = std::move(projectionAsExpr).decompose();
    ExpressionArguments newProjectionArguments = {};
    bool removeNext = false;
    for (auto& arg : asExprDynamics) {
      if (removeNext) {
        removeNext = false;
        continue;
      }
      if (std::holds_alternative<Symbol>(arg)) {
        Symbol symbol = std::get<Symbol>(std::move(arg));
        if (usedSymbols.find(symbol) != usedSymbols.end() || untouchableColumns.find(symbol) != untouchableColumns.end()) {
          newProjectionArguments.emplace_back(std::move(symbol));
        } else {
          removeNext = true;
        }
      } else {
        newProjectionArguments.emplace_back(std::move(arg));
      }
    }
    auto newProjectionAsExpr =
        boss::ComplexExpression(std::move(asExprHead), std::move(asExprStatics),
                                boss::ExpressionArguments(std::move(newProjectionArguments)), std::move(asExprSpans));
    return boss::ComplexExpression(
        std::move(head), std::move(statics),
        boss::ExpressionArguments(std::move(newProjectInput), std::move(newProjectionAsExpr)), std::move(spans));
  } else {
    auto [head, statics, dynamics, spans] = std::move(transformationQuery).decompose();
    for (auto it = std::make_move_iterator(dynamics.begin()); it != std::make_move_iterator(dynamics.end());) {
      if (std::holds_alternative<ComplexExpression>(*it)) {
        auto complexExpr = std::get<ComplexExpression>(std::move(*it));
        *it = std::move(removeUnusedTransformationColumns(std::move(complexExpr), usedSymbols, untouchableColumns));
      }
      ++it;
    }
    return boss::ComplexExpression(std::move(head), std::move(statics), std::move(dynamics), std::move(spans));
  }
}

Expression replaceTransformSymbolsWithQuery(Expression&& expr, Expression&& transformExpression) {
  return std::visit(boss::utilities::overload(
                        [&transformExpression](ComplexExpression&& complexExpr) -> Expression {
                          if (complexExpr.getHead() == "Transformation"_) {
                            return std::move(transformExpression);
                          }
                          auto [head, statics, dynamics, spans] = std::move(complexExpr).decompose();
                          for (auto& arg : dynamics) {
                            arg = replaceTransformSymbolsWithQuery(std::move(arg), std::move(transformExpression));
                          }
                          return boss::ComplexExpression(std::move(head), std::move(statics), std::move(dynamics),
                                                         std::move(spans));
                        },
                        [&transformExpression](Symbol&& symbol) -> Expression {
                          if (symbol == "Transformation"_) {
                            return std::move(transformExpression);
                          }
                          return boss::Expression(std::move(symbol));
                        },
                        [](auto&& otherExpr) -> Expression { return std::forward<decltype(otherExpr)>(otherExpr); }),
                    std::move(expr));
}

// ---------------------------- EXPRESSION PROPAGATION RELATED OPERATIONS END ----------------------------

Expression Engine::processExpression(
    Expression&& inputExpr, std::unordered_map<Symbol, std::unordered_set<Symbol>>& transformationColumnsDependencies,
    std::unordered_set<Symbol>& usedSymbols) {
  return std::visit(
      boss::utilities::overload(
          [this, &transformationColumnsDependencies, &usedSymbols](ComplexExpression&& complexExpr) -> Expression {
            if (complexExpr.getHead() == "Transformation"_) {
              return boss::Expression(std::move(complexExpr));
            } else if (complexExpr.getHead() == "Select"_) {
              std::vector<ComplexExpression> extractedExpressions = {};
              auto expression = extractOperatorsFromSelect(std::move(complexExpr), extractedExpressions,
                                                           transformationColumnsDependencies, usedSymbols);
              for (auto& extractedExpr : extractedExpressions) {
                std::unordered_set<Symbol> extractedExprSymbols = {};
                for (const auto& arg : extractedExpr.getDynamicArguments()) {
                  utilities::getUsedSymbolsFromExpressions(arg, extractedExprSymbols);
                }
                currentTransformationQuery.emplace(moveExctractedSelectExpressionToTransformation(
                    std::move(currentTransformationQuery.value()), std::move(extractedExpr), extractedExprSymbols));
              }
              return std::move(expression);
            } else if (complexExpr.getHead() == "Project"_) {
              // TODO: Think if the project is needed, or can we run getUsedTransformationColumns on the expression
              auto [head, _, dynamics, unused] = std::move(complexExpr).decompose();
              // Process Project's input expression
              auto& projectionInputExpr = std::get<ComplexExpression>(dynamics[0]);
              auto updatedInput = processExpression(std::move(boss::Expression(std::move(projectionInputExpr))),
                                                    transformationColumnsDependencies, usedSymbols);

              // Extract used symbols from the projection function
              auto& projectionAsExpr = std::get<ComplexExpression>(dynamics[1]);
              for (const auto& arg : projectionAsExpr.getDynamicArguments()) {
                utilities::getUsedSymbolsFromExpressions(arg, usedSymbols);
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
                             return processExpression(std::forward<decltype(subExpr)>(subExpr),
                                                      transformationColumnsDependencies, usedSymbols);
                           });
            return boss::ComplexExpression(head, std::move(statics), std::move(dynamics), std::move(spans));
          },
          [this, transformationColumnsDependencies, &usedSymbols](Symbol&& symbol) -> Expression {
            if (utilities::isInTransformationColumns(transformationColumnsDependencies, symbol)) {
              usedSymbols.insert(symbol);
            }
            return boss::Expression(std::move(symbol));
          },
          [](auto&& otherExpr) -> Expression { return std::forward<decltype(otherExpr)>(otherExpr); }),
      std::move(inputExpr));
}
// TODO:
// Add some reverse transformations
// ! 1. Add support for other operators: extraction from Join. Propagation through Join, Order, Group, etc.
// ? 1.1 Propagation is done. Need to add extraction. Also can think of reversing the transformations
// ! 2. Add support for the extraction of OR operator
// ! 3. Add memoization
// ! 4. Evaluate existing code
// ? 5. Speak with Holger?
Expression Engine::evaluate(Expression&& expr) {
  return std::visit(
      boss::utilities::overload(
          [this](ComplexExpression&& infoExpr) -> Expression {
            auto [head, statics, dynamics, spans] = std::move(infoExpr).decompose();
            if (head == "ApplyTransformation"_) {
              if (transformationQueries.size() == 0) {
                return "Error"_("No transformations added");
              }
              int index = 0;
              if (dynamics.size() == 2) {
                index = std::get<int>(std::move(dynamics[1]));
                if (index >= transformationQueries.size() || index < 0) {
                  return "Error"_("Transformation index out of bounds"_);
                }
              }
              currentTransformationQuery.emplace(
                  transformationQueries[index].clone(expressions::CloneReason::EXPRESSION_WRAPPING));
              auto currentUntouchableColumns = transformationsUntouchableColumns[index];
              auto currentColumnDependencies = transformationsColumnDependencies[index];

              ComplexExpression complexExpr = std::get<ComplexExpression>(std::move(dynamics[0]));
              std::unordered_set<Symbol> usedSymbols = {};

              Expression result = processExpression(std::move(complexExpr), currentColumnDependencies, usedSymbols);
              auto allUsedSymbols = utilities::getAllDependentSymbols(currentColumnDependencies, usedSymbols);
              result = removeUnusedTransformationColumns(std::move(currentTransformationQuery.value()), allUsedSymbols,
                                                         currentUntouchableColumns);

              result = replaceTransformSymbolsWithQuery(std::move(result), std::move(currentTransformationQuery.value()));

              return std::move(result);
            } else if (head == "AddTransformation"_) {
              ComplexExpression transformationQuery = std::get<ComplexExpression>(std::move(dynamics[0]));
              std::unordered_map<Symbol, std::unordered_set<Symbol>> dependencyColumns = {};
              std::unordered_set<Symbol> untouchableColumns = {};

              utilities::buildColumnDependencies(transformationQuery, dependencyColumns, untouchableColumns);

              transformationQueries.emplace_back(std::move(transformationQuery));
              transformationsUntouchableColumns.emplace_back(std::move(untouchableColumns));
              transformationsColumnDependencies.emplace_back(std::move(dependencyColumns));

              return "Transformation added successfully"_;
            } else if (head == "GetTransformation"_) {
              if (transformationQueries.size() == 0) {
                return "Error"_("No transformations added"_);
              }
              int index = 0;
              if (dynamics.size() == 1) {
                index = std::get<int>(std::move(dynamics[0]));
                if (index >= transformationQueries.size()) {
                  return "Error"_("Transformation index out of bounds"_);
                }
              }
              return transformationQueries[index].clone(expressions::CloneReason::EXPRESSION_WRAPPING);
            } else if (head == "RemoveTransformation"_) {
              if (transformationQueries.size() == 0) {
                return "Transformation removed successfully"_;
              }
              int index = 0;
              if (dynamics.size() == 1) {
                index = std::get<int>(std::move(dynamics[0]));
                if (index >= transformationQueries.size()) {
                  return "Error"_("Transformation index out of bounds"_);
                }
              }

              if (index >= transformationQueries.size() || index < 0) {
                return "Error"_("Transformation index out of bounds"_);
              }
              transformationQueries.erase(transformationQueries.begin() + index);
              transformationsUntouchableColumns.erase(transformationsUntouchableColumns.begin() + index);
              transformationsColumnDependencies.erase(transformationsColumnDependencies.begin() + index);

              return "Transformation removed successfully"_;
            } else if (head == "RemoveAllTransformations"_) {
              transformationQueries.clear();
              transformationsUntouchableColumns.clear();
              transformationsColumnDependencies.clear();

              return "All transformations removed successfully"_;
            } else if (head == "GetLazyTransformationEngineCapabilities"_) {
              return "List"_("ApplyTransformation"_, "AddTransformation"_, "GetTransformation"_,
                             "RemoveTransformation"_, "RemoveAllTransformations"_);
            }
            std::transform(std::make_move_iterator(dynamics.begin()), std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto&& arg) { return evaluate(std::forward<decltype(arg)>(arg)); });
            return boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
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

static auto& enginePtr(bool initialise = true) {
  static auto engine = std::unique_ptr<boss::engines::LazyTransformation::Engine>();
  if (!engine && initialise) {
    engine.reset(new boss::engines::LazyTransformation::Engine());
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
