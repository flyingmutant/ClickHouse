#include <AggregateFunctions/QuantileBDigest.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>

int main(int, char **)
{
    using namespace DB;

    QuantileBDigest<float> bdigest;
    bdigest.add(1);
    bdigest.add(2);
    bdigest.add(3);
    std::cout << bdigest.get(0.5) << "\n";
    WriteBufferFromOwnString wb;
    bdigest.serialize(wb);
    QuantileBDigest<float> other;
    ReadBufferFromString rb{wb.str()};
    other.deserialize(rb);
    std::cout << other.get(0.5) << "\n";

    return 0;
}
