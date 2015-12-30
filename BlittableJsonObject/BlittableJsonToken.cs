using System;

namespace ConsoleApplication4
{
    [Flags]
    public enum BlittableJsonToken : byte
    {
        StartObject = 1,
        StartArray = 2,
        Integer = 3,
        Float = 4,
        String = 5,
        Boolean = 6,
        Null = 7,

        // Position sizes 
        OffsetSizeByte = 16,
        OffsetSizeShort = 32,
        OffsetSizeInt = 48,

        // PropertyId sizes
        PropertyIdSizeByte = 64,
        PropertyIdSizeShort = 128,
        PropertyIdSizeInt = 192
    }
}