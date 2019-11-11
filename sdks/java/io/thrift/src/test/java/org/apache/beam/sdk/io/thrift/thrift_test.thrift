// This thrift file is used to generate the TestThriftStruct class.

namespace java test_thrift

struct TestThriftStruct {
    1: i8 testByte
    2: i16 testShort
    3: i32 testInt
    4: i64 testLong
    5: double testDouble
    6: map<string, i16> stringIntMap
    7: binary testBinary
    8: bool testBool
}
