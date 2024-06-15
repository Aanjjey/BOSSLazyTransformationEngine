// #include <BOSS.hpp>
// #include <Expression.hpp>
// #include <ExpressionUtilities.hpp>
// #include <Utilities.hpp>
// #include <iostream>
// #include <mutex>
// #include <typeinfo>
// #include <unordered_set>
// #include <variant>

// class BOSSAdditionEngine {
//  public:
//   Expression evaluate(Expression&& e) {
//     return visit(
//         [](auto&& e) -> Expression {
//           if constexpr (isComplexExpression<decltype(e)>) {
//             boss::ExpressionArguments args = e.getArguments();
//             visitTransform(args, [](auto&& arg) -> Expression {
//               if constexpr (isComplexExpression<decltype(arg)>) {
//                 return evaluate(std::move(arg));
//               } else {
//                 return std::forward<decltype(arg)>(arg);
//               }
//             });
//             if (e.getHead() == Symbol("Plus")) {
//               return visitAccumulate(move(args), 0L, [](auto&& state, auto&& arg) {
//                 if constexpr (is_same_v<decay_t<decltype(arg)>, long long>) {
//                   state += arg;
//                 }
//                 return state;
//               });
//             } else {
//               return boss::ComplexExpression(e.getHead(), {}, std::move(args), {});
//             }
//           } else {
//             return forward<decltype(e)>(e);
//           }
//         },
//         move(e));
//   }
// };
