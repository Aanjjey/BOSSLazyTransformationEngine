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

  bool isInTransformationColumns(const Symbol &symbol);
  bool isConditionMoveable(const ComplexExpression &condition);
  std::unordered_set<Symbol> extractTransformationColumns();
  Expression extractOperatorsFromSelect(ComplexExpression &&expr, std::vector<ComplexExpression> &conditionsToMove,
                                        std::unordered_set<Symbol> &projectionColumns);
  void getUsedSymbolsFromExpressions(const Expression &expr, std::unordered_set<Symbol> &usedSymbols);
  void extractOperatorsFromTop(const ComplexExpression &expr);

  Expression extractOperatorsFromExpression(ComplexExpression &&expr, std::vector<ComplexExpression> &selectConditions,
                                            std::unordered_set<Symbol> &projectionColumns);
  Expression processExpression(Expression &&inputExpr, std::vector<ComplexExpression> &selectConditions,
                               std::unordered_set<Symbol> &projectionColumns);

  boss::Expression evaluate(boss::Expression &&e);
};

extern "C" BOSSExpression *evaluate(BOSSExpression *e);
}  // namespace boss::engines::LazyTransformation
