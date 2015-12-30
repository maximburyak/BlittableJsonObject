using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;

namespace NewBlittable
{
    //TODO: implement low memory interface
    //TODO: policy for max size for buffer, after which we cleanup
    public unsafe class UnmanagedBuffersPool : IDisposable
    {
        private readonly ConcurrentDictionary<IntPtr, AllocatedMemoryData> _allocatedSegments =
            new ConcurrentDictionary<IntPtr, AllocatedMemoryData>();

        private readonly ConcurrentDictionary<int, ConcurrentStack<AllocatedMemoryData>> _freeSegments =
            new ConcurrentDictionary<int, ConcurrentStack<AllocatedMemoryData>>();

        private int _allocateMemoryCalls;
        private bool _isDisposed;
        private int _returnMemoryCalls;

        public class AllocatedMemoryData
        {
            public IntPtr Address;
            public string DocumentId;
            public int SizeInBytes;
        }

        ~UnmanagedBuffersPool()
        {
            //TODO: Emit warning to log about un-disposed instance
            Dispose();
        }

        public void Dispose()
        {
            if (_isDisposed)
                return;

            foreach (var allocationQueue in _freeSegments)
            {
                foreach (var mem in allocationQueue.Value)
                {
                    Marshal.FreeHGlobal(mem.Address);
                }
            }

            foreach (var allocatedMemory in _allocatedSegments.Values)
            {
                Marshal.FreeHGlobal(allocatedMemory.Address);
            }
            _freeSegments.Clear();
            _allocatedSegments.Clear();
            _isDisposed = true;
            GC.SuppressFinalize(this);
        }

        /// <summary>
        ///     Allocates memory with the size that is the closes power of 2 to the given size
        /// </summary>
        /// <param name="size">Size to be allocated in bytes</param>
        /// <param name="documentId">Document id to which that memory belongs</param>
        /// <param name="actualSize">The real size of the returned buffer</param>
        /// <returns></returns>
        public byte* GetMemory(int size, string documentId, out int actualSize)
        {
            Interlocked.Increment(ref _allocateMemoryCalls);
            //TODO: Use Utils.NearestPowerOfTwo()
            actualSize = (int) Math.Pow(2, Math.Ceiling(Math.Log(size, 2)));

            AllocatedMemoryData memoryDataForLength;
            ConcurrentStack<AllocatedMemoryData> existingQueue;

            // try get allocated objects queue according to desired size, allocate memory if nothing was not found
            if (_freeSegments.TryGetValue(actualSize, out existingQueue))
            {
                // try de-queue from the allocated memory queue, allocate memory if nothing was returned
                if (existingQueue.TryPop(out memoryDataForLength) == false)
                {
                    memoryDataForLength = new AllocatedMemoryData
                    {
                        SizeInBytes = actualSize,
                        Address = Marshal.AllocHGlobal(actualSize)
                    };
                }
            }
            else
            {
                memoryDataForLength = new AllocatedMemoryData
                {
                    SizeInBytes = actualSize,
                    Address = Marshal.AllocHGlobal(actualSize)
                };
            }
            memoryDataForLength.DocumentId = documentId;

            // document the allocated memory
            if (!_allocatedSegments.TryAdd(memoryDataForLength.Address, memoryDataForLength))
            {
                throw new AccessViolationException(
                    $"Allocated memory at address {memoryDataForLength.Address} was already allocated");
            }

            return (byte*) memoryDataForLength.Address;
        }

        /// <summary>
        ///     Returns allocated memory, which will be stored in the free memory storage
        /// </summary>
        /// <param name="pointer">Pointer to the allocated memory</param>
        public void ReturnMemory(byte* pointer)
        {
            Interlocked.Increment(ref _returnMemoryCalls);
            AllocatedMemoryData memoryDataForPointer;

            if (_allocatedSegments.TryRemove((IntPtr) pointer, out memoryDataForPointer) == false)
            {
                throw new ArgumentException(
                    $"The returned memory pointer {(IntPtr) pointer} was not allocated from this pool, or was already freed",
                    "pointer");
            }

            memoryDataForPointer.DocumentId = null;

            var q = _freeSegments.GetOrAdd(memoryDataForPointer.SizeInBytes, size => new ConcurrentStack<AllocatedMemoryData>());
            q.Push(memoryDataForPointer);
   
        }

        public object GetAllocatedSegments()
        {
            return new
            {
                AllocatedObjects = _allocatedSegments.Values.ToArray(),
                FreeSegments = _freeSegments.SelectMany(x => x.Value.ToArray()).ToArray()
            };
        }

    }
}