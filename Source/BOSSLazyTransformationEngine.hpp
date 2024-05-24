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
                                                    const std::unordered_set<Symbol> &usedSymbols,
                                                    const std::unordered_set<Symbol> &untouchableColumns);

Expression replaceTransformSymbolsWithQuery(Expression &&expr, Expression &&transformationQuery);

class Engine {
 private:
  std::vector<ComplexExpression> transformationQueries;
  std::vector<std::unordered_set<Symbol>> transformationsUntouchableColumns;
  std::vector<std::unordered_map<Symbol, std::unordered_set<Symbol>>> transformationsColumnDependencies;

  std::optional<ComplexExpression> currentTransformationQuery;

 public:
  // Engien is not copyable
  Engine(Engine &) = delete;

  // Engine is not copyable
  Engine &operator=(Engine &) = delete;

  // Engine is movable
  Engine(Engine &&) = default;

  // Engine is not copyable
  Engine &operator=(Engine &&) = delete;

  // Default constructor
  Engine() = default;

  // Default destructor
  ~Engine() = default;

  Expression extractOperatorsFromSelect(
      ComplexExpression &&expr, std::vector<ComplexExpression> &conditionsToMove,
      std::unordered_map<Symbol, std::unordered_set<Symbol>> &transformationColumnsDependencies,
      std::unordered_set<Symbol> &usedSymbols);

  void getUsedSymbolsFromExpressions(const Expression &expr, std::unordered_set<Symbol> &usedSymbols,
                                     bool addAll = false);

  Expression processExpression(
      Expression &&inputExpr, std::unordered_map<Symbol, std::unordered_set<Symbol>> &transformationColumnsDependencies,
      std::unordered_set<Symbol> &usedSymbols);

  boss::Expression evaluate(boss::Expression &&e);
};

extern "C" BOSSExpression *evaluate(BOSSExpression *e);
}  // namespace boss::engines::LazyTransformation
