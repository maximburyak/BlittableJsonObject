using System;
using System.Text;
using Sparrow;
using Voron.Util;

namespace NewBlittable
{
    public unsafe class CompressedStringToByteComparer 
    {
        private readonly BlittableContext _context;
        public readonly byte* Buffer;
        public readonly int _uncompressedSize;
        private readonly int _compressedSize;
        public string String;

        public CompressedStringToByteComparer(string str, byte* buffer, int uncompressedSize, int compressedSize, BlittableContext context)
        {
            String = str;
            _uncompressedSize = uncompressedSize;
            _compressedSize = compressedSize;
            _context = context;
            Buffer = buffer;
        }

        public static implicit operator string(CompressedStringToByteComparer self)
        {
            if (self.String != null)
                return self.String;

            int bufferSize;
            var tempBuffer = self._context.GetTempBuffer(self._uncompressedSize, out bufferSize);
            var uncompressedSize = LZ4.Decode64(self.Buffer, 
                self._compressedSize, 
                tempBuffer, 
                self._uncompressedSize,
                true);

            if (uncompressedSize != self._uncompressedSize)
                throw new FormatException("Wrong size detected on decompression");

            var charCount = self._context.Decoder.GetCharCount(tempBuffer, self._uncompressedSize, true);
            var str = new string(' ', charCount);
            fixed (char* pStr = str)
            {
                self._context.Decoder.GetChars(tempBuffer, self._uncompressedSize, pStr, charCount, true);
                self.String = str;
                return str;
            }
        }

        public override string ToString()
        {
            return (string) this;
        }
    }
}