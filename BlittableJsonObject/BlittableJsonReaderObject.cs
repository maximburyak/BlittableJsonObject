using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using ConsoleApplication4;

namespace NewBlittable
{
    public unsafe class BlittableJsonReaderObject : BlittableJsonReaderBase
    {
        private readonly byte* _propTags;
        private readonly int _propCount;
        private readonly long _currentOffsetSize;
        private readonly long _currentPropertyIdSize;
        private readonly byte* _objStart;

        private Dictionary<string, object> cache;


        public BlittableJsonReaderObject(byte* mem, int size, BlittableContext context)
        {
            _mem = mem; // get beginning of memory pointer
            _size = size; // get document size
            _context = context;

            // init document level properties
            var propStartPos = size - sizeof(int) - sizeof(byte); //get start position of properties
            _propNames = (int*)(mem + (*(int*)(mem + propStartPos)));
            // get pointer to property names array on document level

            // init root level object properties
            var objStartOffset = *(int*)(mem + (size - sizeof(int) - sizeof(int) - sizeof(byte)));
            // get offset of beginning of data of the main object
            byte propCountOffset = 0;
            _propCount = ReadVariableSizeInt(objStartOffset, out propCountOffset); // get main object properties count
            _objStart = objStartOffset + mem;
            _propTags = objStartOffset + mem + propCountOffset;
            // get pointer to current objects property tags metadata collection

            var currentType = (BlittableJsonToken)(*(mem + size - sizeof(byte)));
            // get current type byte flags

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(currentType);
            _currentPropertyIdSize = ProcessTokenPropertyFlags(currentType);
        }

        public BlittableJsonReaderObject(int pos, BlittableJsonReaderBase parent, BlittableJsonToken type)
        {
            _context = parent._context;
            _mem = parent._mem;
            _size = parent._size;
            _propNames = parent._propNames;

            _objStart = _mem + pos;
            byte propCountOffset;
            _propCount = ReadVariableSizeInt(pos, out propCountOffset);
            _propTags = _objStart + propCountOffset;

            // analyze main object type and it's offset and propertyIds flags
            _currentOffsetSize = ProcessTokenOffsetFlags(type);
            _currentPropertyIdSize = ProcessTokenPropertyFlags(type);
        }

        public string[] GetPropertyNames()
        {
            var returnedValue = new string[_propCount];
            var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));
            //TODO: we want to return this in the order they were defined
            for (var i = 0; i < _propCount; i++)
            {
                var propertyIntPtr = (long)_propTags + (i) * metadataSize;
                var propertyId = ReadNumber((byte*)propertyIntPtr + _currentOffsetSize, _currentPropertyIdSize);
                var propName = ReadStringLazily(_propNames[propertyId]);
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

            var comparer = _context.GetComparerFor(name);

            while (min <= max)
            {
                var mid = (min + max) / 2;

                var metadataSize = (_currentOffsetSize + _currentPropertyIdSize + sizeof(byte));
                var propertyIntPtr = (long)_propTags + (mid) * metadataSize;

                var offset = ReadNumber((byte*)propertyIntPtr, _currentPropertyIdSize);
                var propertyId = ReadNumber((byte*)propertyIntPtr + _currentOffsetSize, _currentPropertyIdSize);
                var type =
                    (BlittableJsonToken)
                        ReadNumber((byte*)(propertyIntPtr + _currentOffsetSize + _currentPropertyIdSize),
                            _currentPropertyIdSize);


                var cmpResult = ComparePropertyName(propertyId, comparer);
                if (cmpResult == 0)
                {
                    // found it...
                    result = GetObject(type, (int)((long)_objStart - (long)_mem - (long)offset));
                    if (result is BlittableJsonReaderBase)
                    {
                        if (cache == null)
                        {
                            cache = new Dictionary<string, object>();
                        }
                        cache[name] = result;
                    }
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
           
            return false;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int ComparePropertyName(int propertyId, StringToByteComparer comparer)
        {
            var pos = _propNames[propertyId];
            byte offset;
            var size = ReadVariableSizeInt(pos, out offset);
            return comparer.Compare(_mem + pos + offset, size);
        }

    }
}