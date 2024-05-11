#include "BOSSLazyTransformationEngine.hpp"

#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <ExpressionUtilities.hpp>
#include <Utilities.hpp>
#include <iostream>
#include <mutex>
#include <unordered_set>
#include <variant>

using std::string_literals::operator""s;
using boss::utilities::operator""_;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;

using boss::Expression;

namespace boss::engines::LazyTransformation {

static bool isCardinalityReducingOperator(const Symbol& op) {
  static const std::unordered_set<Symbol> cardinalityReducingOps = {
      "Select"_, "Project"_  //, "Top"_, //"Group"_
  };
  return cardinalityReducingOps.find(op) != cardinalityReducingOps.end();
}

static bool isStaticValue(const Expression& expr) {
  return std::holds_alternative<int32_t>(expr) || std::holds_alternative<int64_t>(expr) ||
         std::holds_alternative<float>(expr) || std::holds_alternative<double>(expr) ||
         std::holds_alternative<bool>(expr) || std::holds_alternative<std::string>(expr) ||
         (std::holds_alternative<ComplexExpression>(expr) &&
          std::get<ComplexExpression>(expr).getHead() == "DateObject"_);
}

Engine::Engine(ComplexExpression&& transformationQuery)
    : transformationQuery(std::move(transformationQuery)), transformationColumns(extractTransformationColumns()) {}

// ! It is used to know which columns from the input query we can extract
// back into the transformation ? Maybe can use some other query scan, so we
// don't have to force to end the transformation with the Project operator
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
        "Transformation query should end with a Project operator in "
        "order to extract available columns");
  }
}

bool Engine::isInTransformationColumns(const Symbol& symbol) {
  return transformationColumns.find(symbol) != transformationColumns.end();
}

// Checks if the condition can be moved to the transformation query by
// checking if the columns in the condition are in the transformation query
// result set or are static values
bool Engine::isConditionMoveable(const ComplexExpression& condition) {
  if (condition.getHead() == "Greater"_ || condition.getHead() == "Equal"_) {
    for (const auto& arg : condition.getDynamicArguments()) {
      if (!(std::holds_alternative<Symbol>(arg) &&
            transformationColumns.find(std::get<Symbol>(arg)) != transformationColumns.end()) &&
          !isStaticValue(arg)) {
        return false;
      }
    }
    return true;
  }
  return false;
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

  auto processedInput = processExpression(std::move(selectDynamics[0]), conditionsToMove, projectionColumns);

  // If the condition is a simple single condition
  if (conditionExpression.getHead() == "Greater"_ || conditionExpression.getHead() == "Equal"_) {
    if (isConditionMoveable(conditionExpression)) {
      // If the single condition is extractable, then we can remove
      // the whole SELECT operator and return its input
      conditionsToMove.emplace_back(std::move(conditionExpression));
      return boss::Expression(std::move(processedInput));
    }
  } else if (conditionExpression.getHead() == "And"_) {
    auto [andHead, unused4, andDynamics, unused5] = std::move(conditionExpression).decompose();
    boss::ExpressionArguments remainingSubconditions = {};
    // If the condition is a complex condition, we need to check each subcondition
    for (auto& andSubcondition : andDynamics) {
      auto& subConditionExpr = std::get<ComplexExpression>(andSubcondition);
      if (isConditionMoveable(subConditionExpr)) {
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
  // Compose and return the original expression
  auto oldWhere =
      boss::ComplexExpression(std::move(whereHead), {}, boss::ExpressionArguments(std::move(conditionExpression)), {});
  auto oldSelect = boss::ComplexExpression(
      std::move(selectHead), {}, boss::ExpressionArguments(std::move(processedInput), std::move(oldWhere)), {});
  return boss::Expression(std::move(oldSelect));
}
// ---------------------------- SELECTION RELATED OPERATIONS START ----------------------------

// ---------------------------- PROJECTION RELATED OPERATIONS START ----------------------------
void Engine::getUsedSymbolsFromExpressions(const Expression& expr, std::unordered_set<Symbol>& usedSymbols) {
  if (std::holds_alternative<Symbol>(expr)) {
    Symbol symbol = std::get<Symbol>(expr);
    if (isInTransformationColumns(symbol)) {
      usedSymbols.insert(symbol);
    }
  } else if (std::holds_alternative<ComplexExpression>(expr)) {
    const auto& complexExpr = std::get<ComplexExpression>(expr);
    for (const auto& arg : complexExpr.getDynamicArguments()) {
      getUsedSymbolsFromExpressions(arg, usedSymbols);
    }
  }
}
// ---------------------------- PROJECTION RELATED OPERATIONS END ----------------------------

void Engine::extractOperatorsFromTop(const ComplexExpression& expr) {
  // TODO: Implement the logic for moving operators to the transformation
}

Expression Engine::extractOperatorsFromExpression(ComplexExpression&& expr,
                                                  std::vector<ComplexExpression>& selectConditions,
                                                  std::unordered_set<Symbol>& projectionColumns) {
  if (expr.getHead() == "Select"_) {
    return std::move(extractOperatorsFromSelect(std::move(expr), selectConditions, projectionColumns));
  } else if (expr.getHead() == "Project"_) {
    auto [head, _, dynamics, unused] = std::move(expr).decompose();
    // Process Project's input expression
    auto& projectionInputExpr = std::get<ComplexExpression>(dynamics[0]);
    auto updatedInput = processExpression(std::move(boss::Expression(std::move(projectionInputExpr))), selectConditions,
                                          projectionColumns);

    // Extract used symbols from the projection function
    auto& projectionAsExpr = std::get<ComplexExpression>(dynamics[1]);
    for (const auto& arg : projectionAsExpr.getDynamicArguments()) {
      getUsedSymbolsFromExpressions(arg, projectionColumns);
    }

    // Compose and return the original expression
    boss::ExpressionArguments&& remainingSubconditions = {};
    remainingSubconditions.emplace_back(std::move(updatedInput));
    remainingSubconditions.emplace_back(boss::Expression(std::move(projectionAsExpr)));
    ComplexExpression resultExpr = boss::ComplexExpression(head, {}, {std::move(remainingSubconditions)}, {});
    return boss::Expression(std::move(resultExpr));
  } else if (expr.getHead() == "Top"_) {
    // extractOperatorsFromTop(expr);
    return boss::Expression(std::move(expr));
  }
  return boss::Expression(std::move(expr));
}

Expression Engine::processExpression(Expression&& inputExpr, std::vector<ComplexExpression>& selectConditions,
                                     std::unordered_set<Symbol>& projectionColumns) {
  if (std::holds_alternative<ComplexExpression>(inputExpr)) {
    ComplexExpression& inputComplexExpr = std::get<ComplexExpression>(inputExpr);

    if (isCardinalityReducingOperator(inputComplexExpr.getHead())) {
      extractOperatorsFromExpression(std::move(inputComplexExpr), selectConditions, projectionColumns);
      // TODO: Implement the logic for moving operators to the transformation
      // moveOperatorsToTransformation(move(inputExpr));
      return std::move(inputExpr);
    }

    auto [head, statics, dynamics, spans] = std::move(inputComplexExpr).decompose();
    // Recursively process sub-expressions
    std::transform(std::make_move_iterator(dynamics.begin()), std::make_move_iterator(dynamics.end()), dynamics.begin(),
                   [&](auto&& subExpr) {
                     return processExpression(std::forward<decltype(subExpr)>(subExpr), selectConditions,
                                              projectionColumns);
                   });
    return boss::ComplexExpression(head, std::move(statics), std::move(dynamics), std::move(spans));
  } else if (std::holds_alternative<Symbol>(inputExpr)) {
    return std::move(inputExpr);
  } else {
    return std::forward<decltype(inputExpr)>(inputExpr);
  }
}

// Thougths:
// 1. Evaluate of the main expression should return either:
//   - Transformation expression followed by a normal expression
//   - Transformation expression inside of a normal expression
Expression Engine::evaluate(Expression&& expr) {
  return std::visit(boss::utilities::overload(
                        [this](ComplexExpression&& complexExpr) -> Expression {
                          std::vector<ComplexExpression> selectConditions = {};
                          std::unordered_set<Symbol> usedSymbols = {};

                          // TODO: will need to return a transformationQuery followed
                          // by a modified user query or
                          //       a modified user query with a transformationQuery
                          //       inside
                          // ? Maybe add a special symbol of the transformation query,
                          // so it can be easily switched into the modified expression
                          Expression result = processExpression(std::move(complexExpr), selectConditions, usedSymbols);
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
                          auto x = "Abracadabra";
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

// // TODO: Check if this is the correct way to handle the engine
// static auto& enginePtr(bool initialise = true) {
//   static auto engine = std::unique_ptr<boss::engines::LazyTransformation::Engine>();
//   if (!engine && initialise) {
//     engine.reset(new boss::engines::LazyTransformation::Engine());
//   }
//   return engine;
// }

// extern "C" BOSSExpression* evaluate(BOSSExpression* e) {
//   static std::mutex m;
//   std::lock_guard lock(m);
//   auto* r = new BOSSExpression{enginePtr()->evaluate(std::move(e->delegate))};
//   return r;
// };

// extern "C" void reset() { enginePtr(false).reset(nullptr); }

// TEST_CASE("Selections are extracted", [select]) {
//   std::vector<boss::ComplexExpression> selectConditions;
//   std::unordered_set<boss::Symbol> usedSymbols;

// Sample input expressions
// boss::Expression selectOnlyExpr = "Select"_(
//     "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6))), "Where"_("Greater"_("A"_, 2)));

// // Expression projectOnlyExpr = "Project"_(
// //     "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6))), "As"_("C"_, "Plus"_("A"_,
// //     "B"_)));

// // Expression complexExpr =
// //     "Project"_("Select"_("Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6))),
// //                          "Where"_("Greater"_("A"_, 2))),
// //                "As"_("C"_, "Plus"_("A"_, "B"_)));

// ComplexExpression transformationExpression = "Project"_(
//     "Table"_("Column"_("A"_, "List"_(1, 2, 3)), "Column"_("B"_, "List"_(4, 5, 6))), "As"_("A", "A", "B", "B"));

// // std::vector<ComplexExpression> operatorsToMove;
// // std::unordered_set<Symbol> transformationColumns{"A"_, "B"_};
// boss::engines::LazyTransformationEngine::Engine engine(std::move(transformationExpression));

// std::cout << "Select conditions:" << std::endl;
// engine.evaluate(std::move(selectOnlyExpr));
// // for (auto const& condition : selectConditions) {
// //   std::cout << condition.toString() << std::endl;
// // }
// }
