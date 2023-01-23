#include <AggregateFunctions/AggregateFunctionQuantile.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>

#include <Core/Field.h>
#include "registerAggregateFunctions.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename Value, bool float_return> using FuncQuantile = AggregateFunctionQuantile<Value, QuantileReservoirSampler<Value>, NameQuantile, false, std::conditional_t<float_return, Float64, void>, false>;
template <typename Value, bool float_return> using FuncQuantiles = AggregateFunctionQuantile<Value, QuantileReservoirSampler<Value>, NameQuantiles, false, std::conditional_t<float_return, Float64, void>, true>;

template <typename Value, bool float_return> using FuncQuantileDeterministic = AggregateFunctionQuantile<Value, QuantileReservoirSamplerDeterministic<Value>, NameQuantileDeterministic, true, std::conditional_t<float_return, Float64, void>, false>;
template <typename Value, bool float_return> using FuncQuantilesDeterministic = AggregateFunctionQuantile<Value, QuantileReservoirSamplerDeterministic<Value>, NameQuantilesDeterministic, true, std::conditional_t<float_return, Float64, void>, true>;

template <typename Value, bool _> using FuncQuantileExact = AggregateFunctionQuantile<Value, QuantileExact<Value>, NameQuantileExact, false, void, false>;
template <typename Value, bool _> using FuncQuantilesExact = AggregateFunctionQuantile<Value, QuantileExact<Value>, NameQuantilesExact, false, void, true>;

template <typename Value, bool _> using FuncQuantileExactExclusive = AggregateFunctionQuantile<Value, QuantileExactExclusive<Value>, NameQuantileExactExclusive, false, Float64, false>;
template <typename Value, bool _> using FuncQuantilesExactExclusive = AggregateFunctionQuantile<Value, QuantileExactExclusive<Value>, NameQuantilesExactExclusive, false, Float64, true>;

template <typename Value, bool _> using FuncQuantileExactInclusive = AggregateFunctionQuantile<Value, QuantileExactInclusive<Value>, NameQuantileExactInclusive, false, Float64, false>;
template <typename Value, bool _> using FuncQuantilesExactInclusive = AggregateFunctionQuantile<Value, QuantileExactInclusive<Value>, NameQuantilesExactInclusive, false, Float64, true>;

template <typename Value, bool _> using FuncQuantileExactWeighted = AggregateFunctionQuantile<Value, QuantileExactWeighted<Value>, NameQuantileExactWeighted, true, void, false>;
template <typename Value, bool _> using FuncQuantilesExactWeighted = AggregateFunctionQuantile<Value, QuantileExactWeighted<Value>, NameQuantilesExactWeighted, true, void, true>;

template <typename Value, bool _> using FuncQuantileTiming = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantileTiming, false, Float32, false>;
template <typename Value, bool _> using FuncQuantilesTiming = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantilesTiming, false, Float32, true>;

template <typename Value, bool _> using FuncQuantileTimingWeighted = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantileTimingWeighted, true, Float32, false>;
template <typename Value, bool _> using FuncQuantilesTimingWeighted = AggregateFunctionQuantile<Value, QuantileTiming<Value>, NameQuantilesTimingWeighted, true, Float32, true>;

template <typename Value, bool float_return> using FuncQuantileTDigest = AggregateFunctionQuantile<Value, QuantileTDigest<Value>, NameQuantileTDigest, false, std::conditional_t<float_return, Float32, void>, false>;
template <typename Value, bool float_return> using FuncQuantilesTDigest = AggregateFunctionQuantile<Value, QuantileTDigest<Value>, NameQuantilesTDigest, false, std::conditional_t<float_return, Float32, void>, true>;

template <typename Value, bool float_return> using FuncQuantileTDigestWeighted = AggregateFunctionQuantile<Value, QuantileTDigest<Value>, NameQuantileTDigestWeighted, true, std::conditional_t<float_return, Float32, void>, false>;
template <typename Value, bool float_return> using FuncQuantilesTDigestWeighted = AggregateFunctionQuantile<Value, QuantileTDigest<Value>, NameQuantilesTDigestWeighted, true, std::conditional_t<float_return, Float32, void>, true>;

template <typename Value, bool float_return> using FuncQuantileBDigest  = AggregateFunctionQuantile<Value, QuantileBDigest<Value>,    NameQuantileBDigest,  false, std::conditional_t<float_return, Float32, void>, false>;
template <typename Value, bool float_return> using FuncQuantileBDigest1 = AggregateFunctionQuantile<Value, QuantileBDigest<Value, 1>, NameQuantileBDigest1, false, std::conditional_t<float_return, Float32, void>, false>;
template <typename Value, bool float_return> using FuncQuantileBDigest2 = AggregateFunctionQuantile<Value, QuantileBDigest<Value, 2>, NameQuantileBDigest2, false, std::conditional_t<float_return, Float32, void>, false>;
template <typename Value, bool float_return> using FuncQuantileBDigest5 = AggregateFunctionQuantile<Value, QuantileBDigest<Value, 5>, NameQuantileBDigest5, false, std::conditional_t<float_return, Float32, void>, false>;
template <typename Value, bool float_return> using FuncQuantilesBDigest  = AggregateFunctionQuantile<Value, QuantileBDigest<Value>,    NameQuantilesBDigest,  false, std::conditional_t<float_return, Float32, void>, true>;
template <typename Value, bool float_return> using FuncQuantilesBDigest1 = AggregateFunctionQuantile<Value, QuantileBDigest<Value, 1>, NameQuantilesBDigest1, false, std::conditional_t<float_return, Float32, void>, true>;
template <typename Value, bool float_return> using FuncQuantilesBDigest2 = AggregateFunctionQuantile<Value, QuantileBDigest<Value, 2>, NameQuantilesBDigest2, false, std::conditional_t<float_return, Float32, void>, true>;
template <typename Value, bool float_return> using FuncQuantilesBDigest5 = AggregateFunctionQuantile<Value, QuantileBDigest<Value, 5>, NameQuantilesBDigest5, false, std::conditional_t<float_return, Float32, void>, true>;
template <typename Value, bool float_return> using FuncQuantileBDigestWeighted  = AggregateFunctionQuantile<Value, QuantileBDigest<Value>,    NameQuantileBDigestWeighted,  true, std::conditional_t<float_return, Float32, void>, false>;
template <typename Value, bool float_return> using FuncQuantileBDigestWeighted1 = AggregateFunctionQuantile<Value, QuantileBDigest<Value, 1>, NameQuantileBDigestWeighted1, true, std::conditional_t<float_return, Float32, void>, false>;
template <typename Value, bool float_return> using FuncQuantileBDigestWeighted2 = AggregateFunctionQuantile<Value, QuantileBDigest<Value, 2>, NameQuantileBDigestWeighted2, true, std::conditional_t<float_return, Float32, void>, false>;
template <typename Value, bool float_return> using FuncQuantileBDigestWeighted5 = AggregateFunctionQuantile<Value, QuantileBDigest<Value, 5>, NameQuantileBDigestWeighted5, true, std::conditional_t<float_return, Float32, void>, false>;
template <typename Value, bool float_return> using FuncQuantilesBDigestWeighted  = AggregateFunctionQuantile<Value, QuantileBDigest<Value>,    NameQuantilesBDigestWeighted,  true, std::conditional_t<float_return, Float32, void>, true>;
template <typename Value, bool float_return> using FuncQuantilesBDigestWeighted1 = AggregateFunctionQuantile<Value, QuantileBDigest<Value, 1>, NameQuantilesBDigestWeighted1, true, std::conditional_t<float_return, Float32, void>, true>;
template <typename Value, bool float_return> using FuncQuantilesBDigestWeighted2 = AggregateFunctionQuantile<Value, QuantileBDigest<Value, 2>, NameQuantilesBDigestWeighted2, true, std::conditional_t<float_return, Float32, void>, true>;
template <typename Value, bool float_return> using FuncQuantilesBDigestWeighted5 = AggregateFunctionQuantile<Value, QuantileBDigest<Value, 5>, NameQuantilesBDigestWeighted5, true, std::conditional_t<float_return, Float32, void>, true>;


template <template <typename, bool> class Function>
static constexpr bool supportDecimal()
{
    return std::is_same_v<Function<Float32, false>, FuncQuantile<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantiles<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantileExact<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantilesExact<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantileExactWeighted<Float32, false>> ||
        std::is_same_v<Function<Float32, false>, FuncQuantilesExactWeighted<Float32, false>>;
}


template <template <typename, bool> class Function>
AggregateFunctionPtr createAggregateFunctionQuantile(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    /// Second argument type check doesn't depend on the type of the first one.
    Function<void, true>::assertSecondArg(argument_types);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return std::make_shared<Function<TYPE, true>>(argument_types, params);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (which.idx == TypeIndex::Date) return std::make_shared<Function<DataTypeDate::FieldType, false>>(argument_types, params);
    if (which.idx == TypeIndex::DateTime) return std::make_shared<Function<DataTypeDateTime::FieldType, false>>(argument_types, params);

    if constexpr (supportDecimal<Function>())
    {
        if (which.idx == TypeIndex::Decimal32) return std::make_shared<Function<Decimal32, false>>(argument_types, params);
        if (which.idx == TypeIndex::Decimal64) return std::make_shared<Function<Decimal64, false>>(argument_types, params);
        if (which.idx == TypeIndex::Decimal128) return std::make_shared<Function<Decimal128, false>>(argument_types, params);
    }

    throw Exception("Illegal type " + argument_type->getName() + " of argument for aggregate function " + name,
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}

}

void registerAggregateFunctionsQuantile(AggregateFunctionFactory & factory)
{
    factory.registerFunction(NameQuantile::name, createAggregateFunctionQuantile<FuncQuantile>);
    factory.registerFunction(NameQuantiles::name, createAggregateFunctionQuantile<FuncQuantiles>);

    factory.registerFunction(NameQuantileDeterministic::name, createAggregateFunctionQuantile<FuncQuantileDeterministic>);
    factory.registerFunction(NameQuantilesDeterministic::name, createAggregateFunctionQuantile<FuncQuantilesDeterministic>);

    factory.registerFunction(NameQuantileExact::name, createAggregateFunctionQuantile<FuncQuantileExact>);
    factory.registerFunction(NameQuantilesExact::name, createAggregateFunctionQuantile<FuncQuantilesExact>);

    factory.registerFunction(NameQuantileExactExclusive::name, createAggregateFunctionQuantile<FuncQuantileExactExclusive>);
    factory.registerFunction(NameQuantilesExactExclusive::name, createAggregateFunctionQuantile<FuncQuantilesExactExclusive>);

    factory.registerFunction(NameQuantileExactInclusive::name, createAggregateFunctionQuantile<FuncQuantileExactInclusive>);
    factory.registerFunction(NameQuantilesExactInclusive::name, createAggregateFunctionQuantile<FuncQuantilesExactInclusive>);

    factory.registerFunction(NameQuantileExactWeighted::name, createAggregateFunctionQuantile<FuncQuantileExactWeighted>);
    factory.registerFunction(NameQuantilesExactWeighted::name, createAggregateFunctionQuantile<FuncQuantilesExactWeighted>);

    factory.registerFunction(NameQuantileTiming::name, createAggregateFunctionQuantile<FuncQuantileTiming>);
    factory.registerFunction(NameQuantilesTiming::name, createAggregateFunctionQuantile<FuncQuantilesTiming>);

    factory.registerFunction(NameQuantileTimingWeighted::name, createAggregateFunctionQuantile<FuncQuantileTimingWeighted>);
    factory.registerFunction(NameQuantilesTimingWeighted::name, createAggregateFunctionQuantile<FuncQuantilesTimingWeighted>);

    factory.registerFunction(NameQuantileTDigest::name, createAggregateFunctionQuantile<FuncQuantileTDigest>);
    factory.registerFunction(NameQuantilesTDigest::name, createAggregateFunctionQuantile<FuncQuantilesTDigest>);

    factory.registerFunction(NameQuantileTDigestWeighted::name, createAggregateFunctionQuantile<FuncQuantileTDigestWeighted>);
    factory.registerFunction(NameQuantilesTDigestWeighted::name, createAggregateFunctionQuantile<FuncQuantilesTDigestWeighted>);

    factory.registerFunction(NameQuantileBDigest::name,  createAggregateFunctionQuantile<FuncQuantileBDigest>);
    factory.registerFunction(NameQuantileBDigest1::name, createAggregateFunctionQuantile<FuncQuantileBDigest1>);
    factory.registerFunction(NameQuantileBDigest2::name, createAggregateFunctionQuantile<FuncQuantileBDigest2>);
    factory.registerFunction(NameQuantileBDigest5::name, createAggregateFunctionQuantile<FuncQuantileBDigest5>);
    factory.registerFunction(NameQuantilesBDigest::name,  createAggregateFunctionQuantile<FuncQuantilesBDigest>);
    factory.registerFunction(NameQuantilesBDigest1::name, createAggregateFunctionQuantile<FuncQuantilesBDigest1>);
    factory.registerFunction(NameQuantilesBDigest2::name, createAggregateFunctionQuantile<FuncQuantilesBDigest2>);
    factory.registerFunction(NameQuantilesBDigest5::name, createAggregateFunctionQuantile<FuncQuantilesBDigest5>);

    factory.registerFunction(NameQuantileBDigestWeighted::name,  createAggregateFunctionQuantile<FuncQuantileBDigestWeighted>);
    factory.registerFunction(NameQuantileBDigestWeighted1::name, createAggregateFunctionQuantile<FuncQuantileBDigestWeighted1>);
    factory.registerFunction(NameQuantileBDigestWeighted2::name, createAggregateFunctionQuantile<FuncQuantileBDigestWeighted2>);
    factory.registerFunction(NameQuantileBDigestWeighted5::name, createAggregateFunctionQuantile<FuncQuantileBDigestWeighted5>);
    factory.registerFunction(NameQuantilesBDigestWeighted::name,  createAggregateFunctionQuantile<FuncQuantilesBDigestWeighted>);
    factory.registerFunction(NameQuantilesBDigestWeighted1::name, createAggregateFunctionQuantile<FuncQuantilesBDigestWeighted1>);
    factory.registerFunction(NameQuantilesBDigestWeighted2::name, createAggregateFunctionQuantile<FuncQuantilesBDigestWeighted2>);
    factory.registerFunction(NameQuantilesBDigestWeighted5::name, createAggregateFunctionQuantile<FuncQuantilesBDigestWeighted5>);

    /// 'median' is an alias for 'quantile'
    factory.registerAlias("median", NameQuantile::name);
    factory.registerAlias("medianDeterministic", NameQuantileDeterministic::name);
    factory.registerAlias("medianExact", NameQuantileExact::name);
    factory.registerAlias("medianExactWeighted", NameQuantileExactWeighted::name);
    factory.registerAlias("medianTiming", NameQuantileTiming::name);
    factory.registerAlias("medianTimingWeighted", NameQuantileTimingWeighted::name);
    factory.registerAlias("medianTDigest", NameQuantileTDigest::name);
    factory.registerAlias("medianTDigestWeighted", NameQuantileTDigestWeighted::name);
    factory.registerAlias("medianBDigest", NameQuantileBDigest::name);
    factory.registerAlias("medianBDigest1", NameQuantileBDigest1::name);
    factory.registerAlias("medianBDigest2", NameQuantileBDigest2::name);
    factory.registerAlias("medianBDigest5", NameQuantileBDigest5::name);
    factory.registerAlias("medianBDigestWeighted", NameQuantileBDigestWeighted::name);
    factory.registerAlias("medianBDigestWeighted1", NameQuantileBDigestWeighted1::name);
    factory.registerAlias("medianBDigestWeighted2", NameQuantileBDigestWeighted2::name);
    factory.registerAlias("medianBDigestWeighted5", NameQuantileBDigestWeighted5::name);
}

}
