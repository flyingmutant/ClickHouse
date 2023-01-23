#pragma once

#include <Common/PODArray.h>
#include <Common/NaNUtils.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int TYPE_MISMATCH;
}


template <typename Value, int relative_error_percent = 1>
struct QuantileBDigest
{
    QuantileBDigest() {
        alpha = relative_error_percent / 100.0;
        gamma = 1 + 2*alpha/(1-alpha);
        gamma_ln = std::log1p(gamma - 1);
    }

    void add(Value x)
    {
        add(x, 1);
    }

    void add(Value x, UInt64 weight)
    {
        if (isNaN(x) || x < 0 || !isFinite(x))
            throw Exception("B-digest can only contain finite non-negative values", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (weight == 0)
            throw Exception("B-digest value weight can not be zero", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (x == 0)
        {
            num_zero += weight;
            return;
        }

        Int64 k = bucketKey(x);
        if (k < 1)
        {
            grow(neg, -k+1);
            neg[-k] += weight;
            num_neg += weight;
        }
        else
        {
            grow(pos, k);
            pos[k-1] += weight;
            num_pos  += weight;
        }
    }

    void merge(const QuantileBDigest & other)
    {
        if (alpha != other.alpha)
            throw Exception("Can not merge b-digests with different relative errors", ErrorCodes::TYPE_MISMATCH);

        grow(neg, other.neg.size());
        for (size_t i = 0; i < other.neg.size(); i++)
            neg[i] += other.neg[i];

        grow(pos, other.pos.size());
        for (size_t i = 0; i < other.pos.size(); i++)
            pos[i] += other.pos[i];

        num_neg  += other.num_neg;
        num_pos  += other.num_pos;
        num_zero += other.num_zero;
    }

    void serialize(WriteBuffer & buf) const
    {
        writeBinary(alpha, buf);
        writeBinary(num_zero, buf);
        writeBinary(UInt32(neg.size()), buf);
        writeBinary(UInt32(pos.size()), buf);
        buf.write(neg.raw_data(), neg.size() * sizeof(UInt64));
        buf.write(pos.raw_data(), pos.size() * sizeof(UInt64));
    }

    void deserialize(ReadBuffer & buf)
    {
        readBinary(alpha, buf);
        if (isNaN(alpha) || alpha <= 0 || alpha >= 1)
            throw Exception("Invalid b-digest relative error value", ErrorCodes::INCORRECT_DATA);

        gamma = 1 + 2*alpha/(1-alpha);
        gamma_ln = std::log1p(gamma - 1);

        UInt32 neg_size = 0;
        UInt32 pos_size = 0;
        readBinary(num_zero, buf);
        readBinary(neg_size, buf);
        readBinary(pos_size, buf);

        neg.resize_exact(neg_size);
        buf.readStrict(const_cast<char *>(neg.raw_data()), neg_size * sizeof(UInt64));
        pos.resize_exact(pos_size);
        buf.readStrict(const_cast<char *>(pos.raw_data()), pos_size * sizeof(UInt64));

        num_neg = 0;
        for (auto n: neg)
            num_neg += n;

        num_pos = 0;
        for (auto n: pos)
            num_pos += n;
    }

    Value get(Float64 level) const
    {
        return getImpl<Value>(level);
    }

    Float32 getFloat(Float64 level) const
    {
        return getImpl<Float32>(level);
    }

    void getMany(const Float64 * levels, const size_t * indices, size_t size, Value * result) const
    {
        getManyImpl<Value>(levels, indices, size, result);
    }

    void getManyFloat(const Float64 * levels, const size_t * indices, size_t size, Float32 * result) const
    {
        getManyImpl<Float32>(levels, indices, size, result);
    }

private:
    static constexpr size_t stack_size = (256 - 3*sizeof(Float64) - 2*sizeof(PODArray<UInt64>) - 3*sizeof(UInt64)) / 2;
    using Array = PODArrayWithStackMemory<UInt64, stack_size>;

private:
    static void grow(Array& a, size_t size) {
        if (size > a.size())
            a.resize_fill(size);
    }

    Int64 bucketKey(Float64 x) const {
        Float64 log_gamma_x = std::log(x) / gamma_ln;
        return std::ceil(log_gamma_x);
    }

    Float64 quantile(Int64 k) const {
        Float64 pow_gamma_k = std::exp(Float64(k) * gamma_ln);
        return 2 * pow_gamma_k / (gamma + 1);
    }

    UInt64 count() const {
        return num_neg + num_pos + num_zero;
    }

    Int64 rankIndexNeg(UInt64 rank) const {
        UInt64 n = 0;
        for (ssize_t i = static_cast<ssize_t>(neg.size()) - 1; i >= 0; i--)
        {
            n += neg[i];
            if (n >= rank)
                return i;
        }

        throw Exception("Impossible b-digest state in rankIndexNeg", ErrorCodes::LOGICAL_ERROR);
    }

    Int64 rankIndexPos(UInt64 rank) const {
        UInt64 n = 0;
        for (size_t i = 0; i < pos.size(); i++)
        {
            n += pos[i];
            if (n >= rank)
                return i;
        }

        throw Exception("Impossible b-digest state in rankIndexPos", ErrorCodes::LOGICAL_ERROR);
    }

    template <typename ResultType>
    ResultType getImpl(Float64 level) const
    {
        if (count() == 0)
            return NaNOrZero<ResultType>();

        UInt64 rank = 1 + level*Float64(count()-1);
        if (rank <= num_zero)
        {
            return 0;
        }
        else if (rank <= num_zero + num_neg)
        {
            Int64 k = rankIndexNeg(rank - num_zero);
            return quantile(-k);
        }
        else
        {
            Int64 k = rankIndexPos(rank - num_zero - num_neg);
            return quantile(k + 1);
        }
    }

    template <typename ResultType>
    void getManyImpl(const Float64 * levels, const size_t * indices, size_t size, ResultType * result) const
    {
        if (count() == 0)
        {
            for (size_t i = 0; i < size; i++)
                result[i] = NaNOrZero<ResultType>();
            return;
        }

        UInt64 n_neg = 0;
        UInt64 n_pos = 0;
        ssize_t i_neg = static_cast<ssize_t>(neg.size()) - 1;
        size_t i_pos = 0;

        for (size_t i = 0; i < size; i++)
        {
            size_t index = indices[i];
            Float64 level = levels[index];
            UInt64 rank = 1 + level*Float64(count()-1);

            if (rank <= num_zero)
            {
                result[index] = 0;
            }
            else if (rank <= num_zero + num_neg)
            {
                UInt64 neg_rank = rank - num_zero;
                for (; i_neg >= 0; i_neg--)
                {
                    if (n_neg + neg[i_neg] >= neg_rank)
                    {
                        result[index] = quantile(-i_neg);
                        break;
                    }
                    n_neg += neg[i_neg];
                }
            }
            else
            {
                UInt64 pos_rank = rank - num_zero - num_neg;
                for (; i_pos < pos.size(); i_pos++)
                {
                    if (n_pos + pos[i_pos] >= pos_rank)
                    {
                        result[index] = quantile(i_pos + 1);
                        break;
                    }
                    n_pos += pos[i_pos];
                }
            }
        }
    }

private:
    Float64 alpha;
    Float64 gamma;
    Float64 gamma_ln;
    Array   neg;
    Array   pos;
    UInt64  num_neg  = 0;
    UInt64  num_pos  = 0;
    UInt64  num_zero = 0;
};

static_assert(sizeof(QuantileBDigest<Float64>) == 256);

}
