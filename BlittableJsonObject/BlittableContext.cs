using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Voron.Util;

namespace NewBlittable
{
    /// <summary>
    /// Single threaded for contexts
    /// </summary>
    public unsafe class BlittableContext : IDisposable
    {
        private readonly UnmanagedBuffersPool _pool;
        private byte* _tempBuffer;
        private int _bufferSize;
        private Dictionary<string, StringToByteComparer> _fieldNames = new Dictionary<string, StringToByteComparer>();
        private bool _disposed;

        public Encoder Encoder;
        public Decoder Decoder;

        public LZ4 Lz4 = new LZ4();

        public BlittableContext(UnmanagedBuffersPool pool)
        {
            _pool = pool;
            _tempBuffer = _pool.GetMemory(128, string.Empty, out _bufferSize);
            Encoder = Encoding.UTF8.GetEncoder();
            Decoder = Encoding.UTF8.GetDecoder();
        }

        /// <summary>
        /// Returns memory buffer to work with, be aware, this buffer is not thread safe
        /// </summary>
        /// <param name="requestedSize"></param>
        /// <param name="actualSize"></param>
        /// <returns></returns>
        public byte* GetTempBuffer(int requestedSize, out int actualSize)
        {
            if (requestedSize > _bufferSize)
            {
                _pool.ReturnMemory(_tempBuffer);
                _tempBuffer = _pool.GetMemory(requestedSize, string.Empty, out _bufferSize);
            }
            actualSize = _bufferSize;
            return _tempBuffer;
        }

        /// <summary>
        /// Generates new unmanaged stream. Should be disposed at the end of the usage.
        /// </summary>
        /// <param name="documentId"></param>
        public UnmanagedWriteBuffer GetStream(string documentId)
        {
            return new UnmanagedWriteBuffer(_pool, documentId);
        }

        public void Dispose()
        {
            if (_disposed)
                return;
            Lz4.Dispose();
            _pool.ReturnMemory(_tempBuffer);
            foreach (var stringToByteComparable in _fieldNames.Values)
            {
                _pool.ReturnMemory(stringToByteComparable.Buffer);
            }
            _disposed = true;
        }

        public StringToByteComparer GetComparerFor(string field)
        {
            StringToByteComparer value;
            if (_fieldNames.TryGetValue(field, out value))
                return value;

            var maxByteCount = Encoding.UTF8.GetMaxByteCount(field.Length);
            int actualSize;
            var memory = _pool.GetMemory(maxByteCount, field, out actualSize);
            fixed (char* pField = field)
            {
                actualSize = Encoder.GetBytes(pField, field.Length, memory, actualSize, true);
                _fieldNames[field] = value = new StringToByteComparer(field, memory, actualSize, this);
            }
            return value;
        }
    }
}
