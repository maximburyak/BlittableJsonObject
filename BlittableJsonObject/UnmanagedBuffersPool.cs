﻿using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Database.Config;

namespace NewBlittable
{
    public unsafe class UnmanagedBuffersPool : IDisposable
    {
        private readonly string _databaseName;
        private readonly int _maxSize;

        private readonly ConcurrentDictionary<IntPtr, AllocatedMemoryData> _allocatedSegments =
            new ConcurrentDictionary<IntPtr, AllocatedMemoryData>();

        private readonly ConcurrentDictionary<int, ConcurrentStack<AllocatedMemoryData>> _freeSegments =
            new ConcurrentDictionary<int, ConcurrentStack<AllocatedMemoryData>>();

        private static readonly ILog log = LogManager.GetCurrentClassLogger();

        private int _allocateMemoryCalls;
        private bool _isDisposed;
        private int _returnMemoryCalls;
        private long _currentSize;

        public class AllocatedMemoryData
        {
            public IntPtr Address;
            public string DocumentId;
            public int SizeInBytes;
        }

        public UnmanagedBuffersPool(string databaseName, int maxSize)
        {
            _databaseName = databaseName;
            _maxSize = maxSize;
        }

        // todo: add test that test concurrent handle low memory and allocations
        public void HandleLowMemory()
        {
            foreach (var key in _freeSegments.Keys)
            {
                ConcurrentStack<AllocatedMemoryData> curKeyStack;
                if (_freeSegments.TryRemove(key, out curKeyStack))
                {
                    AllocatedMemoryData curAllocatedMemoryData;
                    while (curKeyStack.TryPop(out curAllocatedMemoryData))
                    {
                        Marshal.FreeHGlobal(curAllocatedMemoryData.Address);
                        Interlocked.Add(ref _currentSize, curAllocatedMemoryData.SizeInBytes * -1);
                    }
                }
            }
        }

        public void SoftMemoryRelease()
        {
            
        }

        public LowMemoryHandlerStatistics GetStats()
        {
            return new LowMemoryHandlerStatistics
            {
                DatabaseName = _databaseName,
                EstimatedUsedMemory = _currentSize,
                Name = "UnmanagedBufferPool"
            };
        }

        ~UnmanagedBuffersPool()
        {
            if (_isDisposed==false)
                log.Warn("UnmanagedBuffersPool being finalized before it was disposed");
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
                    Interlocked.Add(ref _currentSize, mem.SizeInBytes*-1);
                }
            }

            foreach (var allocatedMemory in _allocatedSegments.Values)
            {
                Marshal.FreeHGlobal(allocatedMemory.Address);
                Interlocked.Add(ref _currentSize, allocatedMemory.SizeInBytes * -1);
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
            actualSize = (int)Utils.GetNextPowerOfTwo(size);

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
                    Interlocked.Add(ref _currentSize, actualSize);
                }
            }
            else
            {
                memoryDataForLength = new AllocatedMemoryData
                {
                    SizeInBytes = actualSize,
                    Address = Marshal.AllocHGlobal(actualSize)
                };
                Interlocked.Add(ref _currentSize, actualSize);
            }
            memoryDataForLength.DocumentId = documentId;

            // document the allocated memory
            if (!_allocatedSegments.TryAdd(memoryDataForLength.Address, memoryDataForLength))
            {
                throw new AccessViolationException(
                    $"Allocated memory at address {memoryDataForLength.Address} was already allocated");
            }
            if (_currentSize >= _maxSize)
                HandleLowMemory();
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