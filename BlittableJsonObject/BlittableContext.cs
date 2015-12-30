using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Sparrow;

namespace NewBlittable
{
    public unsafe class BlittableContext:IDisposable
    {
        private readonly UnmanagedBuffersPool _pool;
        private byte* _bufferPtr;
        private int _bufferSize;
        private readonly Sparrow.Collections.ConcurrentSet<WeakReference<UnmanagedStream>> _streams = new Sparrow.Collections.ConcurrentSet<WeakReference<UnmanagedStream>>();
        public Encoder Encoder;
        public Decoder Decoder;
        public SparrowStringComparer Comparer = new SparrowStringComparer(); 
        private readonly ConcurrentDictionary<string, Tuple<int, byte[]>> _fieldNamesToByteArrays = new ConcurrentDictionary<string, Tuple<int, byte[]>>();

        public BlittableContext(UnmanagedBuffersPool pool, int initialSize)
        {
            _pool = pool;
            _bufferPtr = _pool.GetMemory(initialSize, string.Empty, out _bufferSize);
            Encoder = Encoding.UTF8.GetEncoder();
            Decoder = Encoding.UTF8.GetDecoder();
        }

        /// <summary>
        /// Returns memory buffer to work with, be aware, this buffer is not thread safe
        /// </summary>
        /// <param name="requestedSize"></param>
        /// <param name="actualSize"></param>
        /// <returns></returns>
        public byte* GetBuffer(int requestedSize, out int actualSize)
        {
            if (requestedSize > _bufferSize)
            {
                _pool.ReturnMemory(_bufferPtr);
                _bufferPtr = _pool.GetMemory(_bufferSize + 1, string.Empty, out _bufferSize);
            }
            actualSize = _bufferSize;
            return _bufferPtr;
        }

        /// <summary>
        /// Generates new unmanaged stream. Should be disposed at the end of the usage.
        /// </summary>
        /// <param name="documentId"></param>
        /// <param name="initialSize"></param>
        /// <returns></returns>
        public UnmanagedStream GetStream(string documentId, int initialSize=64)
        {
            var unmanagedStream = new UnmanagedStream(_pool,documentId, initialSize);
            _streams.Add(new WeakReference<UnmanagedStream>(unmanagedStream));
            return unmanagedStream;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int CompareStrings(byte* ptr, int size, string name)
        {
            var byteCount = Encoding.UTF8.GetMaxByteCount(name.Length);
            var memory = _pool.GetMemory(byteCount, name, out byteCount);

            try
            {
                fixed (char* pName = name)
                {
                    var nameSize = Encoder.GetBytes(pName, name.Length, memory, byteCount, true);
                    return MemCompare(ptr, size, memory, nameSize);
                }
            }
            finally
            {
                _pool.ReturnMemory(memory);
            }
        }

        public int CompareStringsWCaching(byte* ptr, int size, string name)
        {
            Tuple<int,byte[]> cachedData;
            cachedData = _fieldNamesToByteArrays.GetOrAdd(name, (str) =>
            {
                var curLength = Encoding.UTF8.GetMaxByteCount(name.Length);
                return Tuple.Create(curLength, new byte[curLength]);
            });
            
            fixed(byte* memory = cachedData.Item2)
            fixed (char* pName = name)
            {
                var nameSize = Encoder.GetBytes(pName, name.Length, memory, cachedData.Item1, true);
                return MemCompare(ptr, size, memory, nameSize);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int MemCompare(byte* ptr, int size, byte* memory, int nameSize)
        {
            var result = Memory.Compare(ptr, memory, Math.Min(size, nameSize));

            if (result == 0)
            {
                return size - nameSize;
            }
            return result;
        }

        public void Dispose()
        {
            _pool.ReturnMemory(_bufferPtr);
            foreach (var weakReference in _streams)
            {
                UnmanagedStream curStream;
                if (weakReference.TryGetTarget(out curStream)) 
                    curStream.Dispose();
            }
        }
    }

    public unsafe class SparrowStringComparer : IComparer<String>
    {
        public int Compare(string y, string x)
        {
            fixed (char* ptr1 = x)
            fixed (char* ptr2 = y)
            {
                return BlittableContext.MemCompare((byte*) ptr1,x.Length, (byte*) ptr2, y.Length);
            }
        }
    }
}
