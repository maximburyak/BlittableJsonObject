using System;
using System.Text;
using Sparrow;

namespace NewBlittable
{
    public unsafe class StringToByteComparer : IComparable<string>, IEquatable<string>,
        IComparable<StringToByteComparer>, IEquatable<StringToByteComparer>
    {
        private readonly BlittableContext _context;
        public readonly byte* Buffer;
        public readonly int Size;
        public string String;

        public StringToByteComparer(string str, byte* buffer, int size, BlittableContext context)
        {
            String = str;
            Size = size;
            _context = context;
            Buffer = buffer;
        }

        public int CompareTo(string other)
        {
            var sizeInBytes = Encoding.UTF8.GetMaxByteCount(other.Length);
            var tmp = _context.GetTempBuffer(sizeInBytes, out sizeInBytes);
            fixed (char* pOther = other)
            {
                var tmpSize = _context.Encoder.GetBytes(pOther, other.Length, tmp, sizeInBytes, true);
                return Compare(tmp, tmpSize);
            }
        }

        public int CompareTo(StringToByteComparer other)
        {
            if (other.Buffer == Buffer && other.Size == Size)
                return 0;
            return Compare(other.Buffer, other.Size);
        }

        public bool Equals(string other)
        {
            return CompareTo(other) == 0;
        }

        public bool Equals(StringToByteComparer other)
        {
            return CompareTo(other) == 0;
        }

        public int Compare(byte* other, int otherSize)
        {
            var result = Memory.Compare(Buffer, other, Math.Min(Size, otherSize));

            return result == 0 ? Size - otherSize : result;
        }

        public static bool operator ==(StringToByteComparer self, string str)
        {
            if (self == null && str == null)
                return true;
            if (self == null || str == null)
                return false;
            return self.Equals(str);
        }

        public static bool operator !=(StringToByteComparer self, string str)
        {
            return !(self == str);
        }

        public static implicit operator string(StringToByteComparer self)
        {
            if (self.String != null)
                return self.String;

            var charCount = self._context.Decoder.GetCharCount(self.Buffer, self.Size, true);
            var str = new string(' ', charCount);
            fixed (char* pStr = str)
            {
                self._context.Decoder.GetChars(self.Buffer, self.Size, pStr, charCount, true);
                self.String = str;
                return str;
            }
        }

        public override bool Equals(object obj)
        {
            var s = obj as string;
            if (s != null)
                return Equals(s);
            var comparer = obj as StringToByteComparer;
            if (comparer != null)
                return Equals(comparer);
            return false;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                const uint fnvPrime = (16777619);
                const uint fnvOffsetBasis = (2166136261);
                uint hash = fnvOffsetBasis;
                for (var i = 0; i < Size; i++)
                {
                    hash ^= Buffer[i];
                    hash *= fnvPrime;
                }
                return (int) hash;
            }
        }

        public override string ToString()
        {
            var thisAsString = (string)this;
            return (string)thisAsString.Clone();
        }

    }
}