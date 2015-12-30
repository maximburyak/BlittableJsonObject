using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NewBlittable
{
    // todo: implement low memory interface
    public unsafe class UnmanagedByteArrayPool:IDisposable
    {
        public struct AllocatedMemoryData
        {
            public long Address;
            public int SizeInBytes;
            public string DocumentId;
        }

        
         static UnmanagedByteArrayPool()
        {
            _staticInstance = new UnmanagedByteArrayPool();
        }
        private static UnmanagedByteArrayPool _staticInstance;
        
        public static UnmanagedByteArrayPool StaticInstance 
        {
            get
            {
                return _staticInstance;
            }
        }

        public UnmanagedByteArrayPool()
        {
        }
        
        private ConcurrentDictionary<int, ConcurrentQueue<AllocatedMemoryData>>  _freeSegments = new ConcurrentDictionary<int, ConcurrentQueue<AllocatedMemoryData>>();
        private ConcurrentDictionary<long, AllocatedMemoryData> _allocatedSegments = new ConcurrentDictionary<long, AllocatedMemoryData>();
        private bool _isDisposed = false;

        /// <summary>
        /// Allocates memory with the size that is the closes power of 2 to the given size
        /// </summary>
        /// <param name="size">size to be allocated in bytes</param>
        /// <param name="documentId">document id to which that memory belongs</param>
        /// <returns></returns>
        unsafe public byte* GetMemory(int size, string documentId, out int actualSize)
        {
            Interlocked.Increment(ref _allocateMemoryCalls);
            actualSize = (int)Math.Pow(2, Math.Ceiling(Math.Log(size, 2)));
            

            AllocatedMemoryData memoryDataForLength;
            ConcurrentQueue<AllocatedMemoryData> existingQueue;

            // try get allocated objects queue according to desired size, allocate memory if nothing was not found
            if (_freeSegments.TryGetValue(actualSize, out existingQueue))
            {
                // try dequeue from the allocated memory queue, allocate memory if nothing was returned
                if (!existingQueue.TryDequeue(out memoryDataForLength))
                {
                    memoryDataForLength = new AllocatedMemoryData();
                    memoryDataForLength.SizeInBytes = actualSize;
                    memoryDataForLength.DocumentId = documentId;
                    memoryDataForLength.Address = (long)Marshal.AllocHGlobal(actualSize);
                //    WriteLog($"Allocated {memoryDataForLength.Address}");
                }
                else
                {
                //    WriteLog($"Returned {memoryDataForLength.Address}");
                }
            }
            else
            {
                memoryDataForLength = new AllocatedMemoryData();
                memoryDataForLength.SizeInBytes = actualSize;
                memoryDataForLength.DocumentId = documentId;
                memoryDataForLength.Address = (long)Marshal.AllocHGlobal(actualSize);
            //    WriteLog($"Allocated {memoryDataForLength.Address}");
            }

            // document the allocated memory
            if (!_allocatedSegments.TryAdd(memoryDataForLength.Address, memoryDataForLength))
            {
                throw new AccessViolationException($"Allocated memory at address {memoryDataForLength.Address} was already allocated");
            }
            
            return (byte*)memoryDataForLength.Address;
        }
        private object logLocker = new object();
        public void WriteLog(string str)
        {
            lock (logLocker)
            {
                Console.WriteLine(str);
            }
        }

        private int _returnMemoryCalls = 0;
        private int _allocateMemoryCalls = 0;
        /// <summary>
        /// Returns allocated memory, which will be stored in the free memory storage
        /// </summary>
        /// <param name="pointer">pointer to the allocated memory</param>
        /// <param name="size">size of the allocated memory</param>
        unsafe public void ReturnMemory(byte* pointer, int size)
        {
            Interlocked.Increment(ref _returnMemoryCalls);
            int powerOfTwoClosestToSize = (int)Math.Pow(2,Math.Ceiling(Math.Log(size, 2)));
            AllocatedMemoryData memoryDataForPointer;
            
            if (_allocatedSegments.TryRemove((long)pointer, out memoryDataForPointer))
            {
                memoryDataForPointer.DocumentId = null;
                _freeSegments.AddOrUpdate(powerOfTwoClosestToSize, x =>
                {
                    var newQueue = new ConcurrentQueue<AllocatedMemoryData>();
                    newQueue.Enqueue(memoryDataForPointer);
                    return newQueue;
                }, (x, queue) =>
                {
                    queue.Enqueue(memoryDataForPointer);
                    return queue;
                });
                
            }
            else
            {
                // ReSharper disable once InvocationIsSkipped
                Console.WriteLine($"AllocateSegmentsCount: {_allocatedSegments.Count}");
                // ReSharper disable once InvocationIsSkipped
                Console.WriteLine($"FreeSegmentsCOunt: {_freeSegments.Count}");
                // ReSharper disable once InvocationIsSkipped
                Console.WriteLine($"Freed Address{(long)pointer}");
                Console.WriteLine($"Return Memory Calls{_returnMemoryCalls}");
                Console.WriteLine($"Allocate Memory Calls{_allocateMemoryCalls}");
                for(var i=0; i <5; i++)
                    Console.WriteLine($"TryGetMemory #{i} result: {_allocatedSegments.ContainsKey((long)pointer)}");
                Debugger.Launch();
                Debugger.Break();
                

                //          throw new ArgumentException($"Memory segments starting at address {(long)pointer} does not exist, cannot return that memory");
            }
        }

        public object GetAllocatedSegments()
        {
            return new
            {
                AllocatedObjects = _allocatedSegments.Values.ToArray(),
                FreeSegments = _freeSegments.SelectMany(x=>x.Value.ToArray()).ToArray()
            };
        }

        ~UnmanagedByteArrayPool()
        {
            Dispose();
        }

        public void Dispose()
        {
            if (!_isDisposed)
            {
                foreach (var allocatedMemory in _freeSegments.SelectMany(x => x.Value))
                {
                    Marshal.FreeHGlobal((IntPtr) allocatedMemory.Address);
                }

                foreach (var allocatedMemory in _allocatedSegments.Values)
                {
                    Marshal.FreeHGlobal((IntPtr) allocatedMemory.Address);
                }
            }
            _isDisposed = true;
        }
    }
}
