#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <cstring>
#include <iostream>
#include <set>
#include <unordered_set>
#include <utility>
#include <vector>

namespace boss::engines::LazyTransformation {

namespace utilities {

bool isCardinalityReducingOperator(const Symbol &op);

bool isStaticValue(const Expression &expr);

bool isInTransformationColumns(const std::unordered_set<Symbol> transformationColumns, const Symbol &symbol);

bool isConditionMoveable(const ComplexExpression &condition, std::unordered_set<Symbol> transformationColumns);

void getUsedSymbolsFromExpressions(const Expression &expr, std::unordered_set<Symbol> &usedSymbols);

void getUsedSymbolsFromExpressions(const Expression &expr, std::unordered_set<Symbol> &usedSymbols,
                                   std::unordered_set<Symbol> &transformationColumns);

std::unordered_set<Symbol> getUsedTransformationColumns(const Expression &expr,
                                                        const std::unordered_set<Symbol> &transformationColumns);

bool canMoveConditionThroughProjection(const ComplexExpression &projectionOperator,
                                       const ComplexExpression &extractedCondition);

bool isOperationReversible(const Expression &expr);

ComplexExpression addConditionToWhereOperator(ComplexExpression &&whereOperator, ComplexExpression &&condition);
}  // namespace utilities

static const Symbol UNEXCTRACTABLE = Symbol("UNEXCTRACTABLE");

class Engine {
 private:
  ComplexExpression transformationQuery;
  const std::unordered_set<Symbol> transformationColumns;

 public:
  // Engien is not copyable
  Engine(Engine &) = delete;

  // Engine is not copyable
  Engine &operator=(Engine &) = delete;

  // Engine is movable
  Engine(Engine &&) = default;

  // Engine is not copyable
  Engine &operator=(Engine &&) = delete;

  // Constructor with transformation query
  Engine(boss::ComplexExpression &&transformationQuery);

  // Default destructor
  ~Engine() = default;

  std::unordered_set<Symbol> extractTransformationColumns();
  Expression extractOperatorsFromSelect(ComplexExpression &&expr, std::vector<ComplexExpression> &conditionsToMove,
                                        std::unordered_set<Symbol> &projectionColumns);
  void getUsedSymbolsFromExpressions(const Expression &expr, std::unordered_set<Symbol> &usedSymbols,
                                     bool addAll = false);
  void extractOperatorsFromTop(const ComplexExpression &expr);

  Expression processExpression(Expression &&inputExpr, std::unordered_set<Symbol> &projectionColumns);
  // void moveExctractedSelectExpressionsToTransformation(ComplexExpression &&extractedExpressions);
  std::unordered_set<Symbol> containsOnlyTransformationColumns(const Expression &expr);

  boss::Expression evaluate(boss::Expression &&e);
};

extern "C" BOSSExpression *evaluate(BOSSExpression *e);
}  // namespace boss::engines::LazyTransformation
