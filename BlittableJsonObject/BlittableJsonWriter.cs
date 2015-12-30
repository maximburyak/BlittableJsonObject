using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using NewBlittable;
using Newtonsoft.Json;
using Raven.Client.Linq.Indexing;
using Sparrow;


namespace ConsoleApplication4
{
    public unsafe class BlittableJsonWriter : IDisposable
    {
        private readonly JsonReader _reader;
        private readonly UnmanagedStream _stream;
        private readonly BlittableContext _context;
        private readonly Dictionary<string, int> _propertyNameToId = new Dictionary<string, int>();
        private readonly List<string> _docPropNames = new List<string>();
        private int _position;
        private byte* _buffer;
        private int _bufferSize = 0;
        private readonly Encoder _encoder;
        

        public BlittableJsonWriter(JsonReader reader, BlittableContext context, string documentId) 
        {
            _reader = reader;
            _stream = context.GetStream(documentId);
            _context = context;
            _encoder = context.Encoder;
            _buffer = _context.GetBuffer(256, out _bufferSize);
        }
        
        public int SizeInBytes
        {
            get { return _stream.SizeInBytes; }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CopyTo(byte* ptr)
        {
            return _stream.CopyTo(ptr);
        }

        public void Write()
        {
            if (_reader.Read() == false)
                throw new EndOfStreamException("Expected start of object, but got EOF");
            if (_reader.TokenType != JsonToken.StartObject)
                throw new InvalidDataException("Expected start of object, but got " + _reader.TokenType);
            BlittableJsonToken token;
            var rootOffset = WriteObject(out token);
            var propertyArrayOffset = new int[_docPropNames.Count];
            for (int index = 0; index < _docPropNames.Count; index++)
            {
                propertyArrayOffset[index] = WriteString(_docPropNames[index]);
            }
            var propertiesStart = _position;
            for (int i = 0; i < propertyArrayOffset.Length; i++)
            {
                WriteNumber(propertyArrayOffset[i], sizeof (int)); // todo: can be made smaller too
            }
            WriteNumber(rootOffset, sizeof (int));
            WriteNumber(propertiesStart, sizeof (int));
            WriteNumber((int) token, sizeof (byte));
        }

        [StructLayout(LayoutKind.Explicit, Size = 9, Pack = 1)]
        public struct PropertyTag
        {
            [FieldOffset(0)] public int Position;
            [FieldOffset(4)] public int PropertyId;
            [FieldOffset(8)] public byte Type;
        }

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


       
        
        private int WriteObject(out BlittableJsonToken objectToken)
        {
            var properties = new List<PropertyTag>();
            var firstWrite = _position;
            int maxPropId = -1;
            while (true)
            {
                if (_reader.Read() == false)
                    throw new EndOfStreamException("Expected property name, but got EOF");

                if (_reader.TokenType == JsonToken.EndObject)
                    break;

                if (_reader.TokenType != JsonToken.PropertyName)
                    throw new InvalidDataException("Expected start of object, but got " + _reader.TokenType);

                var propName = (string) _reader.Value;
                int propIndex;
                if (_propertyNameToId.TryGetValue(propName, out propIndex) == false)
                {
                    propIndex = _propertyNameToId.Count;
                    _propertyNameToId[propName] = propIndex;
                    _docPropNames.Add(propName);
                }

                if (_reader.Read() == false)
                    throw new EndOfStreamException("Expected value, but got EOF");

                BlittableJsonToken token;
                var valuePos = WriteValue(out token);
                maxPropId = Math.Max(maxPropId, propIndex);
                properties.Add(new PropertyTag
                {
                    Position = valuePos,
                    Type = (byte)token,
                    PropertyId = propIndex
                });
            }
            
            var sortedProperties = properties.OrderBy(x => _docPropNames[x.PropertyId], _context.Comparer).ToList();
            var objectPropsStart = _position;
            var distanceFromFirstProperty = objectPropsStart - firstWrite;

            int positionSize, propertyIdSize;
            objectToken = BlittableJsonToken.StartObject;
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

            _position += WriteVariableSizeNumber(sortedProperties.Count);

            foreach (var sortedProperty in sortedProperties)
            {
                WriteNumber(objectPropsStart - sortedProperty.Position, positionSize);
                WriteNumber(sortedProperty.PropertyId, propertyIdSize);
                _stream.WriteByte((byte) sortedProperty.Type);
                _position += positionSize + propertyIdSize + sizeof (byte);
            }

            return objectPropsStart;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int WriteValue(out BlittableJsonToken token)
        {
            int start = _position;
            switch (_reader.TokenType)
            {
                case JsonToken.StartObject:
                    return WriteObject(out token);

                case JsonToken.StartArray:
                    return WriteArray(out token);
                case JsonToken.Integer:
                    _position += WriteVariableSizeNumber((long) _reader.Value);
                    token = BlittableJsonToken.Integer;
                    return start;
                case JsonToken.Float:
                    //TODO: this is probably not very efficient, space wise
                    _position += WriteVariableSizeNumber((long) (double) _reader.Value);
                    token = BlittableJsonToken.Float;
                    return start;
                case JsonToken.String:
                    WriteString((string) _reader.Value);
                    token = BlittableJsonToken.String;
                    return start;
                case JsonToken.Boolean:
                    var value = (byte) ((bool) _reader.Value ? 1 : 0);
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

            int distanceTypeSize;
            arrayToken = BlittableJsonToken.StartArray;

            if (distanceFromFirstItem <= byte.MaxValue)
            {
                distanceTypeSize = sizeof (byte);
                arrayToken |= BlittableJsonToken.OffsetSizeByte;
            }
            else
            {
                if (distanceFromFirstItem <= ushort.MaxValue)
                {
                    distanceTypeSize = sizeof (short);
                    arrayToken |= BlittableJsonToken.OffsetSizeShort;
                }
                else
                {
                    distanceTypeSize = sizeof (int);
                    arrayToken |= BlittableJsonToken.OffsetSizeInt;
                }
            }

            for (int i = 0; i < positions.Count; i++)
            {
                WriteNumber(arrayInfoStart - positions[i], distanceTypeSize);
                _position += distanceTypeSize;
            }

            for (int i = 0; i < types.Count; i++)
            {
                _stream.WriteByte((byte) types[i]);
                _position++;
            }

            return arrayInfoStart;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int WriteString(string str)
        {
            var startPos = _position;
            int byteLen;

            fixed (char* pChars = str)
            {
                var strByteCount = _encoder.GetByteCount(pChars, str.Length, true);

                // enlarge buffer if needed
                if (strByteCount > _bufferSize)
                {
                    _buffer = _context.GetBuffer(strByteCount, out _bufferSize);
                }

                // write amount of bytes the string is going to take
                _position += WriteVariableSizeNumber(strByteCount);
                
                byteLen = _encoder.GetBytes(pChars, str.Length, _buffer, _bufferSize,true);

                if (byteLen != strByteCount)
                    throw new FormatException("calaculated and real byte length did not match, should not happen");
                
                _stream.Write(_buffer, byteLen);
                _position += byteLen;
            }

            return startPos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void WriteNumber(int value, int sizeOfValue)
        {
            switch (sizeOfValue)
            {
                case sizeof (int):
                    _buffer[0] = (byte) value;
                    _buffer[1] = (byte) (value >> 8);
                    _buffer[2] = (byte) (value >> 16);
                    _buffer[3] = (byte) (value >> 24);
                    _stream.Write(_buffer, 4);
                    break;
                case sizeof (short):
                    _buffer[0] = (byte) value;
                    _buffer[1] = (byte) (value >> 8);
                    _stream.Write(_buffer, 2);
                    break;
                case sizeof (byte):
                    _stream.WriteByte((byte) value);
                    break;
                default:
                    throw new ArgumentException($"Unsupported size {sizeOfValue}");
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int WriteVariableSizeNumber(long value)
        {
            int count = 0;
            var v = (ulong)value;
            while (v >= 0x80)
            {
                _buffer[count++] = (byte)(v | 0x80);
                v >>= 7;
            }
            _buffer[count++] = (byte)(v);
            _stream.Write(_buffer, count);
            return count;
        }

        public void Dispose()
        {
            _stream.Dispose();
        }
    }
}