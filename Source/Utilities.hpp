#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <cstring>
#include <iostream>
#include <set>
#include <unordered_set>
#include <utility>
#include <vector>

using boss::ComplexExpression;
using boss::Expression;
using boss::Symbol;

namespace boss::engines::LazyTransformation::utilities {

bool isCardinalityReducingOperator(const Symbol &op);

bool isStaticValue(const Expression &expr);

bool isInTransformationColumns(const std::unordered_map<Symbol, std::unordered_set<Symbol>> &transformationColumns,
                               const Symbol &symbol);

bool isConditionMoveable(const ComplexExpression &condition,
                         std::unordered_map<Symbol, std::unordered_set<Symbol>> &transformationColumns,
                         std::unordered_set<Symbol> &usedSymbols);

void getUsedSymbolsFromExpressions(const Expression &expr, std::unordered_set<Symbol> &usedSymbols);

void getUsedSymbolsFromExpressions(const Expression &expr, std::unordered_set<Symbol> &usedSymbols,
                                   std::unordered_map<Symbol, std::unordered_set<Symbol>> &transformationColumns);

std::unordered_set<Symbol> getUsedTransformationColumns(
    const Expression &expr, const std::unordered_map<Symbol, std::unordered_set<Symbol>> &transformationColumns);

std::unordered_set<Symbol> getAllDependentSymbols(
    const std::unordered_map<Symbol, std::unordered_set<Symbol>> &transformationColumnsDependencies,
    const std::unordered_set<Symbol> &usedSymbols);

bool canMoveConditionThroughProjection(const ComplexExpression &projectionOperator,
                                       const ComplexExpression &extractedCondition);

bool isOperationReversible(const Expression &expr);

void buildColumnDependencies(const ComplexExpression &expr,
                             std::unordered_map<Symbol, std::unordered_set<Symbol>> &transformationColumnsDependencies,
                             std::unordered_set<Symbol> &untouchableColumns);

ComplexExpression mergeConsecutiveSelectOperators(ComplexExpression &&outerSelect);

ComplexExpression addConditionToWhereOperator(ComplexExpression &&whereOperator, ComplexExpression &&condition);
}  // namespace boss::engines::LazyTransformation::utilities
