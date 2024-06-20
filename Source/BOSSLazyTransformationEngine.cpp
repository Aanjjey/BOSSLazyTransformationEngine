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
  // Subprocess the input expression
  auto processedInput = processExpression(std::move(selectDynamics[0]), transformationColumnsDependencies, usedSymbols);

  // If the condition is a simple single condition
  if (conditionExpression.getHead() == "Greater"_ || conditionExpression.getHead() == "Equal"_) {
    // First value represents if the value is extractable. Second is if the condition should be added to inner
    // Union, Intersect, Except, Difference operators
    std::vector<bool> result =
        utilities::isConditionMoveable(processedInput, conditionExpression, transformationColumnsDependencies, usedSymbols);
    if (result[0]) {
      if (result[1]) {
        processedInput = wrapNestedSetOperatorsWithSelect(std::move(processedInput), std::move(conditionExpression));
      }

      // If the single condition is extractable, then we can remove the whole SELECT operator and return its input
      conditionsToMove.emplace_back(std::move(conditionExpression));
      return boss::Expression(std::move(processedInput));
    }
  } else if (conditionExpression.getHead() == "And"_) {
    // You can always extract from the AND operator, unless AND is inside of the OR operator
    // If you have an OR operator, then you can extract the condition if the condition on that column is present
    // in all OR branches
    auto [andHead, unused4, andDynamics, unused5] = std::move(conditionExpression).decompose();
    boss::ExpressionArguments remainingSubconditions = {};
    // If the condition is a complex condition, we need to check each subcondition
    for (auto& andSubcondition : andDynamics) {
      auto subConditionExpr = std::get<ComplexExpression>(std::move(andSubcondition));
      std::vector<bool> result =
          utilities::isConditionMoveable(processedInput, subConditionExpr, transformationColumnsDependencies, usedSymbols);
      if (result[0]) {
        if (result[1]) {
          processedInput = wrapNestedSetOperatorsWithSelect(std::move(processedInput), std::move(conditionExpression));
        }
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
  } else if (conditionExpression.getHead() == "Or"_) {
    // If the condition is an OR operator, we need to check if the condition is extractable from each branch
    auto [orHead, unused6, orDynamics, unused7] = std::move(conditionExpression).decompose();
    std::vector<std::unordered_set<Symbol>> allOrBranches;
    bool isFirstBranch = true;
    for (const auto& orSubcondition : orDynamics) {
      auto& subConditionExpr = std::get<ComplexExpression>(orSubcondition);
      std::vector<std::unordered_set<Symbol>> currentBranchConditions = {};
      if (subConditionExpr.getHead() == "And"_) {
        for (const auto& andSubcondition : subConditionExpr.getDynamicArguments()) {
          auto& subConditionExpr = std::get<ComplexExpression>(andSubcondition);
          std::vector<bool> result = utilities::isConditionMoveable(processedInput, subConditionExpr,
                                                                    transformationColumnsDependencies, usedSymbols);
          if (result[0]) {
            std::unordered_set<Symbol> conditionSymbols = {};
            utilities::getUsedSymbolsFromExpressions(andSubcondition, conditionSymbols);
            currentBranchConditions.emplace_back(std::move(conditionSymbols));
          }
        }
      } else if (subConditionExpr.getHead() == "Greater"_ || subConditionExpr.getHead() == "Equal"_) {
        std::vector<bool> result =
            utilities::isConditionMoveable(processedInput, subConditionExpr, transformationColumnsDependencies, usedSymbols);
        if (result[0]) {
          std::unordered_set<Symbol> conditionSymbols = {};
          utilities::getUsedSymbolsFromExpressions(orSubcondition, conditionSymbols);
          currentBranchConditions.emplace_back(std::move(conditionSymbols));
        }
      } else {
        allOrBranches.clear();
        break;
      }
      if (isFirstBranch) {
        for (auto& condition : currentBranchConditions) {
          allOrBranches.emplace_back(std::move(condition));
        }
        isFirstBranch = false;
      } else {
        std::vector<std::unordered_set<Symbol>> newOrBranches = {};
        for (auto& intersection : allOrBranches) {
          for (auto& currentBranchCondition : currentBranchConditions) {
            bool isIntersection = true;
            for (const auto& symbol : currentBranchCondition) {
              if (intersection.find(symbol) != intersection.end()) {
                isIntersection = false;
              }
            }
            if (isIntersection) {
              newOrBranches.emplace_back(std::move(intersection));
            }
          }
        }
      }
    }
    if (allOrBranches.size() == 0) {
      // If there are no common conditions in all branches, then we can't extract the condition
      return boss::Expression(std::move(expr));
    } else {
      // If there are common conditions in all branches, we need to extract them
      boss::ExpressionArguments extractedOrConditions = {};
      boss::ExpressionArguments remainingOrSubconditions = {};
      for (auto& orSubcondition : orDynamics) {
        auto subConditionExpr = std::get<ComplexExpression>(std::move(orSubcondition));
        if (subConditionExpr.getHead() == "And"_) {
          auto [andHead, unused8, andDynamics, unused9] = std::move(subConditionExpr).decompose();
          boss::ExpressionArguments remainingAndSubconditions = {};
          boss::ExpressionArguments extractedAndConditions = {};
          for (auto& andSubcondition : andDynamics) {
            std::unordered_set<Symbol> conditionSymbols = {};
            utilities::getUsedSymbolsFromExpressions(andSubcondition, conditionSymbols);
            bool isExtractable = false;
            for (auto allOrBranch : allOrBranches) {
              bool allConditions = true;
              for (const auto& symbol : conditionSymbols) {
                if (allOrBranch.find(symbol) == allOrBranch.end()) {
                  allConditions = false;
                  break;
                }
              }
              if (allConditions) {
                isExtractable = true;
                break;
              }
            }
            if (isExtractable) {
              extractedAndConditions.emplace_back(std::move(andSubcondition));
            } else {
              remainingAndSubconditions.emplace_back(std::move(andSubcondition));
            }
          }
          if (extractedAndConditions.size() == 1) {
            extractedOrConditions.emplace_back(std::move(extractedAndConditions[0]));
          } else {
            extractedOrConditions.emplace_back(
                boss::ComplexExpression(std::move(andHead), {}, std::move(extractedAndConditions), {}));
          }

          if (remainingAndSubconditions.size() == 1) {
            remainingOrSubconditions.emplace_back(std::move(remainingAndSubconditions[0]));
          } else if (remainingAndSubconditions.size() > 1) {
            remainingOrSubconditions.emplace_back(
                boss::ComplexExpression("And"_, {}, std::move(remainingAndSubconditions), {}));
          }
        } else if (subConditionExpr.getHead() == "Greater"_ || subConditionExpr.getHead() == "Equal"_) {
          // If the condition is a simple single condition, then it was verified before for extraction (allOrBranches != 0)
          extractedOrConditions.emplace_back(std::move(subConditionExpr));
        }
      }
      auto newExtractedOr = boss::ComplexExpression(std::move(orHead), {}, std::move(extractedOrConditions), {});
      conditionsToMove.emplace_back(std::move(newExtractedOr));
      if (remainingOrSubconditions.size() == 0) {
        // If all subconditions are extractable, then we can remove the whole SELECT operator and return its input
        return boss::Expression(std::move(processedInput));
      } else {
        if (remainingOrSubconditions.size() == 1) {
          // If size is one, we don't need to recreate the whole OR operator
          auto newWhere = boss::ComplexExpression(std::move(whereHead), {},
                                                  boss::ExpressionArguments(std::move(remainingOrSubconditions[0])), {});
          auto newSelect = boss::ComplexExpression(
              std::move(selectHead), {}, boss::ExpressionArguments(std::move(processedInput), std::move(newWhere)), {});
          return boss::Expression(std::move(newSelect));
        } else {
          auto newRemainingOr = boss::ComplexExpression("Or"_, {}, std::move(remainingOrSubconditions), {});
          auto newWhere =
              boss::ComplexExpression(std::move(whereHead), {}, boss::ExpressionArguments(std::move(newRemainingOr)), {});
          auto newSelect = boss::ComplexExpression(
              std::move(selectHead), {}, boss::ExpressionArguments(std::move(processedInput), std::move(newWhere)), {});
          return boss::Expression(std::move(newSelect));
        }
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

Expression wrapNestedSetOperatorsWithSelect(Expression&& expr, ComplexExpression&& condition) {
  if (std::holds_alternative<ComplexExpression>(expr)) {
    auto transformingExpression = std::get<ComplexExpression>(std::move(expr));
    if (transformingExpression.getHead() == "Union"_ || transformingExpression.getHead() == "Intersect"_ ||
        transformingExpression.getHead() == "Except"_ || transformingExpression.getHead() == "Difference"_) {
      auto [head, statics, dynamics, spans] = std::move(transformingExpression).decompose();
      ExpressionArguments newArguments = {};
      for (auto& arg : dynamics) {
        std::unordered_set<Symbol> usedSymbols = {};
        utilities::getUsedSymbolsFromExpressions(arg, usedSymbols);
        if (usedSymbols.find("Transformation"_) == usedSymbols.end()) {
          newArguments.emplace_back(utilities::wrapOperatorWithSelect(std::move(arg), condition.clone()));
        } else {
          // Since condition is already extracted into transformation, no need to wrap it again.
          // Need to check though if that other branch of the operator has its own set operators
          newArguments.emplace_back(wrapNestedSetOperatorsWithSelect(std::move(arg), condition.clone()));
        }
      }
      auto newExpression =
          boss::ComplexExpression(std::move(head), std::move(statics), std::move(newArguments), std::move(spans));
      return std::move(newExpression);
    } else {
      auto [head, statics, dynamics, spans] = std::move(transformingExpression).decompose();
      ExpressionArguments newArguments = {};
      for (auto& arg : dynamics) {
        newArguments.emplace_back(wrapNestedSetOperatorsWithSelect(std::move(arg), condition.clone()));
      }
      auto newExpression =
          boss::ComplexExpression(std::move(head), std::move(statics), std::move(newArguments), std::move(spans));
      return std::move(newExpression);
    }
  } else {
    return std::move(expr);
  }
}
// ---------------------------- EXPRESSION EXTRACTION RELATED OPERATIONS END ----------------------------

// ---------------------------- EXPRESSION PROPAGATION RELATED OPERATIONS START ----------------------------

ComplexExpression moveExctractedSelectExpressionToTransformation(Expression&& expression,
                                                                 ComplexExpression&& extractedExpression,
                                                                 const std::unordered_set<Symbol>& extractedExprSymbols) {
  if (std::holds_alternative<ComplexExpression>(expression)) {
    auto transformingExpression = std::get<ComplexExpression>(std::move(expression));
    if (transformingExpression.getHead() == "Select"_) {
      // Push through the SELECT operator
      auto [head, statics, dynamics, spans] = std::move(transformingExpression).decompose();
      auto selectInput = std::move(dynamics[0]);
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
        auto projectInput = std::move(dynamics[0]);
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
      auto expressionInput = std::move(dynamics[0]);
      auto newExpressionInput = moveExctractedSelectExpressionToTransformation(
          std::move(expressionInput), std::move(extractedExpression), extractedExprSymbols);
      return boss::ComplexExpression(std::move(head), std::move(statics),
                                     boss::ExpressionArguments(std::move(newExpressionInput), std::move(dynamics[1])),
                                     std::move(spans));

    } else if (transformingExpression.getHead() == "Union"_ || transformingExpression.getHead() == "Intersect"_ ||
               transformingExpression.getHead() == "Except"_ || transformingExpression.getHead() == "Difference"_) {
      // So far seems like only Union is supported in BOSS. But I guess these are what other would be called in the
      // future Am not fully sure on the naming between Except and Difference, so I include both
      auto [head, statics, dynamics, spans] = std::move(transformingExpression).decompose();
      ExpressionArguments newArguments = {};
      // Need to push to all inputs of the expressions
      for (auto& arg : dynamics) {
        // Need to clone the extracted expression as it will be consumed by each input
        newArguments.emplace_back(moveExctractedSelectExpressionToTransformation(
            std::move(arg), std::move(extractedExpression.clone(expressions::CloneReason::EXPRESSION_WRAPPING)),
            extractedExprSymbols));
      }
      return boss::ComplexExpression(std::move(head), std::move(statics), std::move(newArguments), std::move(spans));
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
          auto expressionInput = std::move(dynamics[0]);
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

      // Check that they are not both true or both false (both true should not possible for valid input)
      if (canPushThrough1 != canPushThrough2) {
        auto [head, statics, dynamics, spans] = std::move(transformingExpression).decompose();
        if (canPushThrough1) {
          auto newJoinInput1 = moveExctractedSelectExpressionToTransformation(
              std::move(dynamics[0]), std::move(extractedExpression), extractedExprSymbols);
          return boss::ComplexExpression(
              std::move(head), std::move(statics),
              boss::ExpressionArguments(std::move(newJoinInput1), std::move(dynamics[1]), std::move(dynamics[2])),
              std::move(spans));
        } else if (canPushThrough2) {
          auto newJoinInput2 = moveExctractedSelectExpressionToTransformation(
              std::move(dynamics[1]), std::move(extractedExpression), extractedExprSymbols);
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
  } else {
    auto newWhere = boss::ComplexExpression("Where"_, {}, boss::ExpressionArguments(std::move(extractedExpression)), {});
    auto newSelect =
        boss::ComplexExpression("Select"_, {}, boss::ExpressionArguments(std::move(expression), std::move(newWhere)), {});
    return std::move(newSelect);
  }
}

// Remove unused columns from the Group operator
ComplexExpression removeUnusedTransformationColumns(ComplexExpression&& transformationQuery,
                                                    const std::unordered_set<Symbol>& usedSymbols,
                                                    const std::unordered_set<Symbol>& untouchableColumns) {
  // "As" is found in "Project" and "Group" operators which also modify the columns. Since their working principle is
  // the same, we can handle them in the same way. We need to remove the unused column and the expression after it
  // e.g. As(A, sum(B), C, sum(D)) -> As(A, sum(B))
  if (transformationQuery.getHead() == "As"_) {
    auto [asExprHead, asExprStatics, asExprDynamics, asExprSpans] = std::move(transformationQuery).decompose();
    ExpressionArguments newAsProjectionArguments = {};
    bool removeNext = false;
    for (auto& arg : asExprDynamics) {
      if (removeNext) {
        removeNext = false;
        continue;
      }
      if (std::holds_alternative<Symbol>(arg)) {
        Symbol symbol = std::get<Symbol>(std::move(arg));
        if (usedSymbols.find(symbol) != usedSymbols.end() || untouchableColumns.find(symbol) != untouchableColumns.end()) {
          newAsProjectionArguments.emplace_back(std::move(symbol));
        } else {
          removeNext = true;
        }
      } else {
        newAsProjectionArguments.emplace_back(std::move(arg));
      }
    }
    return boss::ComplexExpression(std::move(asExprHead), std::move(asExprStatics),
                                   boss::ExpressionArguments(std::move(newAsProjectionArguments)), std::move(asExprSpans));
  } else {
    ExpressionArguments newArguments = {};
    auto [head, statics, dynamics, spans] = std::move(transformationQuery).decompose();
    for (auto& arg : dynamics) {
      if (std::holds_alternative<ComplexExpression>(arg)) {
        newArguments.emplace_back(removeUnusedTransformationColumns(std::move(std::get<ComplexExpression>(std::move(arg))),
                                                                    usedSymbols, untouchableColumns));
      } else {
        newArguments.emplace_back(std::move(arg));
      }
    }
    return boss::ComplexExpression(std::move(head), std::move(statics), boss::ExpressionArguments(std::move(newArguments)),
                                   std::move(spans));
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
                currentTransformationQuery = std::move(moveExctractedSelectExpressionToTransformation(
                    std::move(currentTransformationQuery), std::move(extractedExpr), extractedExprSymbols));
              }
              return std::move(expression);
            } else if (complexExpr.getHead() == "Project"_) {
              auto [head, _, dynamics, unused] = std::move(complexExpr).decompose();
              // Process Project's input expression
              auto& projectionInputExpr = dynamics[0];
              auto updatedInput = processExpression(std::move(std::move(projectionInputExpr)),
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
            if (symbol == "Transformation"_) {
              return boss::Expression(std::move(symbol));
            }
            if (utilities::isInTransformationColumns(transformationColumnsDependencies, symbol)) {
              usedSymbols.insert(symbol);
            }
            return boss::Expression(std::move(symbol));
          },
          [](auto&& otherExpr) -> Expression { return std::forward<decltype(otherExpr)>(otherExpr); }),
      std::move(inputExpr));
}

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
              currentTransformationQuery =
                  std::move(transformationQueries[index].clone(expressions::CloneReason::EXPRESSION_WRAPPING));
              auto currentUntouchableColumns = transformationsUntouchableColumns[index];
              auto currentColumnDependencies = transformationsColumnDependencies[index];

              ComplexExpression complexExpr = std::get<ComplexExpression>(std::move(dynamics[0]));
              std::unordered_set<Symbol> usedSymbols = {};

              Expression result = processExpression(std::move(complexExpr), currentColumnDependencies, usedSymbols);
              auto allUsedSymbols = utilities::getAllDependentSymbols(currentColumnDependencies, usedSymbols);
              currentTransformationQuery = std::move(removeUnusedTransformationColumns(
                  std::move(currentTransformationQuery), allUsedSymbols, currentUntouchableColumns));

              result = replaceTransformSymbolsWithQuery(std::move(result), std::move(currentTransformationQuery));

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
              return "List"_("ApplyTransformation"_, "AddTransformation"_, "GetTransformation"_, "RemoveTransformation"_,
                             "RemoveAllTransformations"_);
            }
            std::transform(std::make_move_iterator(dynamics.begin()), std::make_move_iterator(dynamics.end()),
                           dynamics.begin(), [this](auto&& arg) { return evaluate(std::forward<decltype(arg)>(arg)); });
            return boss::ComplexExpression(std::move(head), {}, std::move(dynamics), std::move(spans));
          },
          [this](Symbol&& symbol) -> Expression { return std::move(symbol); },
          [](auto&& otherExpr) -> Expression { return std::forward<decltype(otherExpr)>(otherExpr); }),
      std::move(expr));
}
}  // namespace boss::engines::LazyTransformation

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
