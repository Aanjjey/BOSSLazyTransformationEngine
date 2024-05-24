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

static const Symbol UNEXCTRACTABLE = Symbol("UNEXCTRACTABLE");

ComplexExpression moveExctractedSelectExpressionToTransformation(ComplexExpression &&transformingExpression,
                                                                 ComplexExpression &&extractedExpressions);

ComplexExpression removeUnusedTransformationColumns(ComplexExpression &&transformationQuery,
                                                    const std::unordered_set<Symbol> &usedSymbols);

class Engine {
 private:
  ComplexExpression permanentTransformationQuery;
  ComplexExpression changingTransformationQuery;
  std::unordered_set<Symbol> untouchableColumns;
  std::unordered_map<Symbol, std::unordered_set<Symbol>> transformationColumnsDependencies;

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

  Expression extractOperatorsFromSelect(ComplexExpression &&expr, std::vector<ComplexExpression> &conditionsToMove,
                                        std::unordered_set<Symbol> &projectionColumns);
  void getUsedSymbolsFromExpressions(const Expression &expr, std::unordered_set<Symbol> &usedSymbols,
                                     bool addAll = false);
  void extractOperatorsFromTop(const ComplexExpression &expr);

  Expression replaceTransformSymbolsWithQuery(Expression &&expr);

  Expression processExpression(Expression &&inputExpr, std::unordered_set<Symbol> &usedSymbols);

  boss::Expression evaluate(boss::Expression &&e);
};

extern "C" BOSSExpression *evaluate(BOSSExpression *e);
}  // namespace boss::engines::LazyTransformation
