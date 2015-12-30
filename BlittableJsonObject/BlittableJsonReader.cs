using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using ConsoleApplication4;
using Sparrow;

namespace NewBlittable
{
    public unsafe class BlittableJsonReaderBase
    {
        private const byte OffsetSizeMask = 48;
        private const byte PropertyIdSizeMask = 192;
        private const byte BlittableJsonTypesMask = 15;

        internal byte* _mem;
        internal int _size;
        internal int* _propNames;
        internal BlittableContext _context;

        public void ProcessTokenPropertyFlags(BlittableJsonWriter.BlittableJsonToken currentType,
            out long currentPropertyIdSize)
        {
            // process part of byte flags that responsible for property ids sizes
            var propertyIdSizeEnum = (BlittableJsonWriter.BlittableJsonToken) ((byte) currentType & PropertyIdSizeMask);
            switch (propertyIdSizeEnum)
            {
                case BlittableJsonWriter.BlittableJsonToken.PropertyIdSizeByte:
                    currentPropertyIdSize = sizeof (byte);
                    break;
                case BlittableJsonWriter.BlittableJsonToken.PropertyIdSizeShort:
                    currentPropertyIdSize = sizeof (short);
                    break;
                case BlittableJsonWriter.BlittableJsonToken.PropertyIdSizeInt:
                    currentPropertyIdSize = sizeof (int);
                    break;
                default:
                    throw new ArgumentException("Illegal offset size");
            }
        }

        public void ProcessTokenOffsetFlags(BlittableJsonWriter.BlittableJsonToken currentType, out long currentOffsetSize)
        {
            // process part of byte flags that responsible for offset sizes
            var offsetSizeEnum = (BlittableJsonWriter.BlittableJsonToken) ((byte) currentType & OffsetSizeMask);
            switch (offsetSizeEnum)
            {
                case BlittableJsonWriter.BlittableJsonToken.OffsetSizeByte:
                    currentOffsetSize = sizeof (byte);
                    break;
                case BlittableJsonWriter.BlittableJsonToken.OffsetSizeShort:
                    currentOffsetSize = sizeof (short);
                    break;
                case BlittableJsonWriter.BlittableJsonToken.OffsetSizeInt:
                    currentOffsetSize = sizeof (int);
                    break;
                default:
                    throw new ArgumentException("Illegal offset size");
            }
        }

        internal object GetObject(BlittableJsonWriter.BlittableJsonToken type, int position)
        {
            switch ((BlittableJsonWriter.BlittableJsonToken) ((byte) type & BlittableJsonTypesMask))
            {
                case BlittableJsonWriter.BlittableJsonToken.StartObject:
                    return new BlittableJsonReaderObject(position, this, type);
                case BlittableJsonWriter.BlittableJsonToken.StartArray:
                    return new BlittableJsonReaderArray(position, this, type);
                case BlittableJsonWriter.BlittableJsonToken.Integer:
                    return ReadVariableSizeInt(position);
                case BlittableJsonWriter.BlittableJsonToken.String:
                    //TODO: Return lazily created object, implementing == without materializing the string
                    //TODO: and allowing to index this directly without creating the string
                    return ReadStringMaterialized(position);
                case BlittableJsonWriter.BlittableJsonToken.Boolean:
                    return (byte) ReadNumber(_mem + position, 1);
                case BlittableJsonWriter.BlittableJsonToken.Null:
                    return null;
                case BlittableJsonWriter.BlittableJsonToken.Float:
                    return (double) ReadVariableSizeLong(position);
                default:
                    throw new ArgumentOutOfRangeException((type).ToString());
            }
        }

        public int ReadNumber(byte* value, long sizeOfValue)
        {
            var returnValue = 0;
            switch (sizeOfValue)
            {
                case sizeof (int):
                    returnValue = *value;
                    returnValue |= *(value + 1) << 8;
                    returnValue |= *(value + 2) << 16;
                    returnValue |= *(value + 3) << 24;
                    return returnValue;
                case sizeof (short):
                    returnValue = *value;
                    returnValue |= *(value + 1) << 8;
                    return returnValue;
                case sizeof (byte):
                    returnValue = *value;
                    return returnValue;
                default:
                    throw new ArgumentException($"Unsupported size {sizeOfValue}");
            }
        }

        public class NonMaterializedComparisonString
        {
            private readonly byte* _ptr;
            private readonly int _size;
            private readonly BlittableContext _context;
            private string _materializedInstance;

            public NonMaterializedComparisonString(byte* ptr, int size, BlittableContext context)
            {
                _ptr = ptr;
                _size = size;
                _context = context;
            }

            public int Compare(string str)
            {
                return _context.CompareStrings(_ptr, _size, str);
            }

            public  static bool operator ==(NonMaterializedComparisonString nmStr, string str)
            {
                if (nmStr == null && str == null)
                    return true;
                if (nmStr == null || str == null)
                    return false;
                return nmStr.Compare(str) == 0;
            }

            public static bool operator !=(NonMaterializedComparisonString nmStr, string str)
            {
                if (nmStr == null && str == null)
                    return false;
                if (nmStr == null || str == null)
                    return true;
                return nmStr.Compare(str) != 0;
            }

            public static implicit operator string(NonMaterializedComparisonString nmstr)
            {
                return nmstr.ReadStringMaterialized();
            }
           
            public string ReadStringMaterialized()
            {
                if (_materializedInstance != null)
                    return _materializedInstance;
                
                var charCount = _context.Decoder.GetCharCount(_ptr, _size, true);
                var str = new string(' ', charCount);
                fixed (char* ch = str)
                {
                    _context.Decoder.GetChars(_ptr, _size, ch, charCount, true);
                }
                return _materializedInstance = str;
            }
        }
        public string ReadStringMaterialized(int pos)
        {
            byte offset = 0;
            var size = ReadVariableSizeInt(pos, out offset);

            var charCount = _context.Decoder.GetCharCount(_mem + pos + offset, size, true);
            var str = new string(' ', charCount);
            fixed (char* ch = str)
            {
                _context.Decoder.GetChars(_mem + pos + offset, size, ch, charCount, true);
            }
            return str;
        }

        public int ReadVariableSizeInt(int pos, out byte offset)
        {
            // Read out an Int32 7 bits at a time.  The high bit 
            // of the byte when on means to continue reading more bytes.
            offset = 0;
            int count = 0;
            int shift = 0;
            byte b;
            do
            {
                if (shift == 35)
                    throw new FormatException("Bad variable size int");
                b = _mem[pos++];
                count |= (b & 0x7F) << shift;
                shift += 7;
                offset++;
            } while ((b & 0x80) != 0);
            return count;
        }

        protected int ReadVariableSizeInt(int pos)
        {
            // Read out an Int32 7 bits at a time.  The high bit 
            // of the byte when on means to continue reading more bytes.
            int count = 0;
            int shift = 0;
            byte b;
            do
            {
                if (shift == 35)
                    throw new FormatException("Bad variable size int");
                b = _mem[pos++];
                count |= (b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            return count;
        }

        protected long ReadVariableSizeLong(int pos)
        {
            // Read out an Int32 7 bits at a time.  The high bit 
            // of the byte when on means to continue reading more bytes.
            long count = 0;
            int shift = 0;
            byte b;
            do
            {
                if (shift == 35)
                    throw new FormatException("Bad variable size int");
                b = _mem[pos++];
                count |= (b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            return count;
        }
    }

    public unsafe class BlittableJsonReaderObject : BlittableJsonReaderBase
    {
        private byte* _propTags;
        private int _propCount;
        private long _currentOffsetSize;
        private long _currentPropertyIdSize;
        private byte* _objStart;
        private Dictionary<string, object> cache;


        public BlittableJsonReaderObject(byte* mem, int size, BlittableContext context)
        {
            _mem = mem; // get beginning of memory pointer
            _size = size; // get document size
            _context = context;

            // init document lelvel properties
            var propStartPos = size - sizeof (int) - sizeof (byte); //get start position of properties
            _propNames = (int*) (mem + (*(int*) (mem + propStartPos)));
                // get pointer to proprty names array on document level

            // init root level object properties
            var objStartOffset = *(int*) (mem + (size - sizeof (int) - sizeof (int) - sizeof (byte)));
            // get offset of beginning of data of the main object
            byte propCountOffset = 0;
            _propCount = ReadVariableSizeInt(objStartOffset, out propCountOffset); // get main object properties count
            _objStart = objStartOffset + mem;
            _propTags = objStartOffset + mem + propCountOffset;
            // get pointer to current objects property tags metadata collection

            var currentType = (BlittableJsonWriter.BlittableJsonToken) (*(mem + size - sizeof (byte)));
            // get current type byte flags

            // analyze main object type and it's offset and propertyIds flags
            ProcessTokenOffsetFlags(currentType, out _currentOffsetSize);
            ProcessTokenPropertyFlags(currentType, out _currentPropertyIdSize);
        }

        public BlittableJsonReaderObject(int pos, BlittableJsonReaderBase parent, BlittableJsonWriter.BlittableJsonToken type)
        {
            _context = parent._context;
            _mem = parent._mem;
            _size = parent._size;
            _propNames = parent._propNames;

            _objStart = _mem + pos;
            byte propCountOffset = 0;
            _propCount = ReadVariableSizeInt(pos, out propCountOffset);
            _propTags = _objStart + propCountOffset;

            // analyze main object type and it's offset and propertyIds flags
            ProcessTokenOffsetFlags(type, out _currentOffsetSize);
            ProcessTokenPropertyFlags(type, out _currentPropertyIdSize);
        }

        public string[] GetPropertyNames()
        {
            var returnedValue = new string[_propCount];
            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof (byte));

            for (var i = 0; i < _propCount; i++)
            {
                var propertyIntPtr = (long) _propTags + (i)*metadataSize;
                var propertyId = ReadNumber((byte*) propertyIntPtr + _currentOffsetSize, _currentPropertyIdSize);
                var propName = ReadStringMaterialized(_propNames[propertyId]);
                returnedValue[i] = propName;
            }
            return returnedValue;
        }

        public object this[string name]
        {
            get
            {
                object result = null;
                if (TryGetMember(name, out result) == false)
                    throw new ArgumentException($"Member named {name} does not exist");
                return result;
            }
        }

        public bool TryGetMember(string name, out object result)
        {
            result = null;
            int min = 0, max = _propCount;

            // try get value from cache, works only with Blittable types, other objects are not stored for now
            if (cache != null && cache.TryGetValue(name, out result))
                return true;

            while (min <= max)
            {
                var mid = (min + max)/2;

                var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof (byte));
                var propertyIntPtr = (long) _propTags + (mid)*metadataSize;

                var offset = ReadNumber((byte*) propertyIntPtr, _currentPropertyIdSize);
                var propertyId = ReadNumber((byte*) propertyIntPtr + _currentOffsetSize, _currentPropertyIdSize);
                var type =
                    (BlittableJsonWriter.BlittableJsonToken)
                        ReadNumber((byte*) (propertyIntPtr + _currentOffsetSize + _currentPropertyIdSize),
                            _currentPropertyIdSize);


                var cmpResult = ComparePropertyName(propertyId, name);
                if (cmpResult == 0)
                {
                    // found it...
                    result = GetObject(type, (int) ((long) _objStart - (long) _mem - (long) offset));
                    return true;
                }
                if (cmpResult > 0)
                {
                    min = mid + 1;
                }
                else
                {
                    max = mid - 1;
                }
            }

            if (result is BlittableJsonReaderBase)
            {
                if (cache == null)
                {
                    cache = new Dictionary<string, object>();
                }
                cache.Add(name, result);
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ComparePropertyName(int propertyId, string name)
        {
            byte offset = 0;
            var pos = _propNames[propertyId];
            var size = ReadVariableSizeInt(pos, out offset);
            return _context.CompareStringsWCaching(_mem + pos + offset, size, name);
        }
    }

    public unsafe class BlittableJsonReaderArray : BlittableJsonReaderBase
    {
        private BlittableJsonReaderBase _parent;
        private int _count;
        private byte* _positions;
        private byte* _types;
        private byte* _dataStart;
        private long _currentOffsetSize;
        private Dictionary<int, object> cache;

        public BlittableJsonReaderArray(int pos, BlittableJsonReaderBase parent, BlittableJsonWriter.BlittableJsonToken type)
        {
            _parent = parent;
            byte arraySizeOffset = 0;
            _count = parent.ReadVariableSizeInt(pos, out arraySizeOffset);

            _dataStart = parent._mem + pos;
            _positions = parent._mem + pos + arraySizeOffset;

            // analyze main object type and it's offset and propertyIds flags
            ProcessTokenOffsetFlags(type, out _currentOffsetSize);

            _types = parent._mem + pos + arraySizeOffset + _count*_currentOffsetSize;
        }

        public int Length => _count;

        public int Count => _count;

        public object this[int index]
        {
            get
            {
                object result = null;
                TryGetIndex(index, out result);
                return result;
            }
        }

        public bool TryGetIndex(int index, out object result)
        {
            result = null;

            // try get value from cache, works only with Blittable types, other objects are not stored for now
            if (cache != null && cache.TryGetValue(index, out result))
                return true;

            if (index >= _count || index < 0)
                return false;
            var memAsIntPtr = (long) _parent._mem;
            var dataStartIntPtr = (long) _dataStart;

            var offset = ReadNumber(_positions + index*_currentOffsetSize, _currentOffsetSize);
            result = _parent.GetObject((BlittableJsonWriter.BlittableJsonToken) _types[index],
                (int) (dataStartIntPtr - memAsIntPtr - offset));

            if (result is BlittableJsonReaderBase)
            {
                if (cache == null)
                {
                    cache = new Dictionary<int, object>();
                }
                cache.Add(index, result);
            }
            return true;
        }
       
    }
}