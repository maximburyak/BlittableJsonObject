using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using NewBlittable;
using Newtonsoft.Json;

namespace ConsoleApplication4
{
    public unsafe class BlittableJsonWriter : IDisposable
    {
        private readonly BlittableContext _context;
        private readonly List<StringToByteComparer> _docPropNames = new List<StringToByteComparer>();
        private readonly Encoder _encoder;
        private readonly Dictionary<string, int> _propertyNameToId = new Dictionary<string, int>();
        private readonly JsonReader _reader;
        private readonly UnmanagedWriteBuffer _stream;
        private int _bufferSize = 128;
        private int _position;

        public BlittableJsonWriter(JsonReader reader, BlittableContext context, string documentId)
        {
            _reader = reader;
            _stream = context.GetStream(documentId);
            _context = context;
            _encoder = context.Encoder;
        }

        public int SizeInBytes
        {
            get { return _stream.SizeInBytes; }
        }

        public void Dispose()
        {
            _stream.Dispose();
        }

        private byte* GetTempBuffer(int minSize)
        {
            // enlarge buffer if needed
            if (minSize > _bufferSize)
            {
                _bufferSize = (int)Utils.GetNextPowerOfTwo(minSize);
            }
            return _context.GetTempBuffer(_bufferSize, out _bufferSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CopyTo(byte* ptr)
        {
            return _stream.CopyTo(ptr);
        }

        /// <summary>
        /// Writes the json object from  reader received in the ctor into the received UnmanangedWriteBuffer
        /// </summary>
        public void Write()
        {
            if (_reader.Read() == false)
                throw new EndOfStreamException("Expected start of object, but got EOF");
            if (_reader.TokenType != JsonToken.StartObject)
                throw new InvalidDataException("Expected start of object, but got " + _reader.TokenType);
            BlittableJsonToken token;

            // Write the whole object recursively
            var rootOffset = WriteObject(out token);

            // Write the property names and register it's positions
            var propertyArrayOffset = new int[_docPropNames.Count];
            for (var index = 0; index < _docPropNames.Count; index++)
            {
                propertyArrayOffset[index] = WriteString(_docPropNames[index]);
            }

            // Register the position of the properties offsets start
            var propertiesStart = _position;

            // Find the minimal space to store the offsets (byte,short,int) and raise the appropriate flag in the properties metadata
            BlittableJsonToken propertiesSizeMetadata = 0;
            var propertyNamesOffset = _position - rootOffset;
            var propertyArrayOffsetValueByteSize = SetOffsetSizeFlag(ref propertiesSizeMetadata, propertyNamesOffset);
            
            WriteNumber((int)propertiesSizeMetadata, sizeof(byte));

            // Write property names offsets
            for (var i = 0; i < propertyArrayOffset.Length; i++)
            {
                WriteNumber(propertiesStart - propertyArrayOffset[i], propertyArrayOffsetValueByteSize);
            }
            WriteNumber(rootOffset, sizeof(int));
            WriteNumber(propertiesStart, sizeof(int));
            WriteNumber((int)token, sizeof(byte));
        }

        /// <summary>
        /// Write an object to the UnamangedBuffer
        /// </summary>
        /// <param name="objectToken"></param>
        /// <returns></returns>
        private int WriteObject(out BlittableJsonToken objectToken)
        {
            var properties = new List<PropertyTag>();
            var firstWrite = _position;
            var maxPropId = -1;

            // Iterate through the object's properties, write it to the UnmanagedWriteBuffer and register it's names and positions
            while (true)
            {
                if (_reader.Read() == false)
                    throw new EndOfStreamException("Expected property name, but got EOF");

                if (_reader.TokenType == JsonToken.EndObject)
                    break;

                if (_reader.TokenType != JsonToken.PropertyName)
                    throw new InvalidDataException("Expected start of object, but got " + _reader.TokenType);

                // Get property index by name
                var propName = (string)_reader.Value;
                int propIndex;
                if (_propertyNameToId.TryGetValue(propName, out propIndex) == false)
                {
                    propIndex = _propertyNameToId.Count;
                    _propertyNameToId[propName] = propIndex;
                    _docPropNames.Add(_context.GetComparerFor(propName));
                }

                maxPropId = Math.Max(maxPropId, propIndex);

                if (_reader.Read() == false)
                    throw new EndOfStreamException("Expected value, but got EOF");

                // Write object property into the UnmanagedWriteBuffer
                BlittableJsonToken token;
                var valuePos = WriteValue(out token);
                
                // Register property possition, name id (PropertyId) and type (object type and metadata)
                properties.Add(new PropertyTag
                {
                    Position = valuePos,
                    Type = (byte)token,
                    PropertyId = propIndex
                });
            }

            // Sort object properties metadata by property names
            properties.Sort(CompareProperties);
            
            var objectMetadataStart = _position;
            var distanceFromFirstProperty = objectMetadataStart - firstWrite;

            // Find metadata size and properties offset and set appropriate flags in the BlittableJsonToken
            objectToken = BlittableJsonToken.StartObject;
            var positionSize = SetOffsetSizeFlag(ref objectToken, distanceFromFirstProperty);
            var propertyIdSize = SetPropertyIdSizeFlag(ref objectToken, maxPropId);

            _position += WriteVariableSizeNumber(properties.Count);

            // Write object metadata
            foreach (var sortedProperty in properties)
            {
                WriteNumber(objectMetadataStart - sortedProperty.Position, positionSize);
                WriteNumber(sortedProperty.PropertyId, propertyIdSize);
                _stream.WriteByte(sortedProperty.Type);
                _position += positionSize + propertyIdSize + sizeof(byte);
            }

            return objectMetadataStart;
        }

        private static int SetPropertyIdSizeFlag(ref BlittableJsonToken objectToken, int maxPropId)
        {
            int propertyIdSize;
            if (maxPropId <= byte.MaxValue)
            {
                propertyIdSize = sizeof (byte);
                objectToken |= BlittableJsonToken.PropertyIdSizeByte;
            }
            else
            {
                if (maxPropId <= ushort.MaxValue)
                {
                    propertyIdSize = sizeof (short);
                    objectToken |= BlittableJsonToken.PropertyIdSizeShort;
                }
                else
                {
                    propertyIdSize = sizeof (int);
                    objectToken |= BlittableJsonToken.PropertyIdSizeInt;
                }
            }
            return propertyIdSize;
        }

        private static int SetOffsetSizeFlag(ref BlittableJsonToken objectToken, int distanceFromFirstProperty)
        {
            int positionSize;
            if (distanceFromFirstProperty <= byte.MaxValue)
            {
                positionSize = sizeof (byte);
                objectToken |= BlittableJsonToken.OffsetSizeByte;
            }
            else
            {
                if (distanceFromFirstProperty <= ushort.MaxValue)
                {
                    positionSize = sizeof (short);
                    objectToken |= BlittableJsonToken.OffsetSizeShort;
                }
                else
                {
                    positionSize = sizeof (int);
                    objectToken |= BlittableJsonToken.OffsetSizeInt;
                }
            }
            return positionSize;
        }

        private int CompareProperties(PropertyTag x, PropertyTag y)
        {
            return _docPropNames[x.PropertyId].CompareTo(_docPropNames[y.PropertyId]);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int WriteValue(out BlittableJsonToken token)
        {
            var start = _position;
            switch (_reader.TokenType)
            {
                case JsonToken.StartObject:
                    return WriteObject(out token);

                case JsonToken.StartArray:
                    return WriteArray(out token);
                case JsonToken.Integer:
                    _position += WriteVariableSizeNumber((long)_reader.Value);
                    token = BlittableJsonToken.Integer;
                    return start;
                case JsonToken.Float:
                    //TODO: this is probably not very efficient, space wise
                    _position += WriteVariableSizeNumber((long)(double)_reader.Value);
                    token = BlittableJsonToken.Float;
                    return start;
                case JsonToken.String:
                    WriteString((string)_reader.Value);
                    token = BlittableJsonToken.String;
                    return start;
                case JsonToken.Boolean:
                    var value = (byte)((bool)_reader.Value ? 1 : 0);
                    _stream.WriteByte(value);
                    _position++;
                    token = BlittableJsonToken.Boolean;
                    return start;
                case JsonToken.Null:
                    token = BlittableJsonToken.Null;
                    return start; // nothing to do here, we handle that with the token
                case JsonToken.Undefined:
                    token = BlittableJsonToken.Null;
                    return start; // nothing to do here, we handle that with the token
                case JsonToken.Date:
                    throw new NotImplementedException("Writing /*dates*/ is not supported");
                case JsonToken.Bytes:
                    throw new NotImplementedException("Writing bytes is not supported");
                // ReSharper disable RedundantCaseLabel
                case JsonToken.PropertyName:
                case JsonToken.None:
                case JsonToken.StartConstructor:
                case JsonToken.EndConstructor:
                case JsonToken.EndObject:
                case JsonToken.EndArray:
                case JsonToken.Raw:
                case JsonToken.Comment:
                default:
                    throw new InvalidDataException("Expected a value, but got " + _reader.TokenType);
                    // ReSharper restore RedundantCaseLabel
            }
        }

        private int WriteArray(out BlittableJsonToken arrayToken)
        {
            var positions = new List<int>();
            var types = new List<BlittableJsonToken>();
            while (true)
            {
                if (_reader.Read() == false)
                    throw new EndOfStreamException("Expected value, but got EOF");
                if (_reader.TokenType == JsonToken.EndArray)
                    break;


                BlittableJsonToken token;
                var pos = WriteValue(out token);
                types.Add(token);
                positions.Add(pos);
            }
            var arrayInfoStart = _position;

            var distanceFromFirstItem = arrayInfoStart - positions[0];
            _position += WriteVariableSizeNumber(positions.Count);

            
            arrayToken = BlittableJsonToken.StartArray;

            var distanceTypeSize = SetOffsetSizeFlag(ref arrayToken, distanceFromFirstItem);

            for (var i = 0; i < positions.Count; i++)
            {
                WriteNumber(arrayInfoStart - positions[i], distanceTypeSize);
                _position += distanceTypeSize;
            }

            for (var i = 0; i < types.Count; i++)
            {
                _stream.WriteByte((byte)types[i]);
                _position++;
            }

            return arrayInfoStart;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteString(string str)
        {
            var startPos = _position;
            int byteLen;

            fixed (char* pChars = str)
            {
                var strByteCount = _encoder.GetByteCount(pChars, str.Length, true);

                // write amount of bytes the string is going to take
                _position += WriteVariableSizeNumber(strByteCount);
                var buffer = GetTempBuffer(strByteCount);
                byteLen = _encoder.GetBytes(pChars, str.Length, buffer, _bufferSize, true);

                if (byteLen != strByteCount)
                    throw new FormatException("calaculated and real byte length did not match, should not happen");

                _stream.Write(buffer, byteLen);
                _position += byteLen;
            }

            return startPos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteNumber(int value, int sizeOfValue)
        {
            var buffer = GetTempBuffer(8);
            switch (sizeOfValue)
            {
                case sizeof(int):
                    buffer[0] = (byte)value;
                    buffer[1] = (byte)(value >> 8);
                    buffer[2] = (byte)(value >> 16);
                    buffer[3] = (byte)(value >> 24);
                    _stream.Write(buffer, 4);
                    break;
                case sizeof(short):
                    buffer[0] = (byte)value;
                    buffer[1] = (byte)(value >> 8);
                    _stream.Write(buffer, 2);
                    break;
                case sizeof(byte):
                    _stream.WriteByte((byte)value);
                    break;
                default:
                    throw new ArgumentException($"Unsupported size {sizeOfValue}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteVariableSizeNumber(long value)
        {
            var buffer = GetTempBuffer(8);
            var count = 0;
            var v = (ulong)value;
            while (v >= 0x80)
            {
                buffer[count++] = (byte)(v | 0x80);
                v >>= 7;
            }
            buffer[count++] = (byte)(v);
            _stream.Write(buffer, count);
            return count;
        }

        [StructLayout(LayoutKind.Explicit, Size = 9, Pack = 1)]
        public struct PropertyTag
        {
            [FieldOffset(0)]
            public int Position;
            [FieldOffset(4)]
            public int PropertyId;
            [FieldOffset(8)]
            public byte Type;
        }
    }
}