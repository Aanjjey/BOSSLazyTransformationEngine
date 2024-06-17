#include <BOSS.hpp>
#include <Engine.hpp>
#include <Expression.hpp>
#include <cstring>
#include <iostream>
#include <set>
#include <unordered_set>
#include <utility>
#include <vector>

using std::string_literals::operator""s;
using boss::ComplexExpression;
using boss::Span;
using boss::Symbol;

using boss::Expression;  // NOLINT(misc-unused-using-decls)

namespace boss::engines::LazyTransformation {

static const Symbol UNEXCTRACTABLE = Symbol("UNEXCTRACTABLE");
static ComplexExpression UNEXCTRACTABLE_EXPRESSION = ComplexExpression(UNEXCTRACTABLE, {}, {}, {});

ComplexExpression moveExctractedSelectExpressionToTransformation(Expression &&transformingExpression,
                                                                 ComplexExpression &&extractedExpressions,
                                                                 const std::unordered_set<Symbol> &usedSymbols);

ComplexExpression removeUnusedTransformationColumns(ComplexExpression &&transformationQuery,
                                                    const std::unordered_set<Symbol> &usedSymbols,
                                                    const std::unordered_set<Symbol> &untouchableColumns);

Expression replaceTransformSymbolsWithQuery(Expression &&expr, Expression &&transformationQuery);

class Engine {
 private:
  std::vector<ComplexExpression> transformationQueries;
  std::vector<std::unordered_set<Symbol>> transformationsUntouchableColumns;
  std::vector<std::unordered_map<Symbol, std::unordered_set<Symbol>>> transformationsColumnDependencies;
  //   std::unordered_map<Expression, Expression> transformationCache;
  //   bool isTransformationCacheEnabled = false;
  //   bool cacheCurrentTransformationQuery = false;

  ComplexExpression currentTransformationQuery1 = UNEXCTRACTABLE_EXPRESSION.clone();

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
