using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace NewBlittable.Tests
{
    public unsafe class UnmanagedStreamTests
    {
        [Fact]
        public void BulkWriteAscendingSizeTest()
        {
            using (var unmanagedByteArrayPool = new UnmanagedByteArrayPool())
            {
                List<Tuple<long, int>> allocatedMemory = new List<Tuple<long, int>>();
                var newStream = new UnmanagedStream(unmanagedByteArrayPool, "trolo", 64, 8);
                var totalSize = 0;
                var rand = new Random();
                var curSize = 0;
                for (var i = 1; i < 5000; i++)
                {
                    var pointer = unmanagedByteArrayPool.GetMemory(rand.Next(1, i*7), string.Empty, out curSize);
                    totalSize += curSize;
                    FillData(pointer, curSize);
                    allocatedMemory.Add(Tuple.Create((long) pointer, curSize));
                    newStream.Write(pointer, curSize);
                }

                var buffer = unmanagedByteArrayPool.GetMemory(newStream.SizeInBytes, string.Empty, out curSize);

                var copiedSize = newStream.CopyTo(buffer);
                Assert.Equal(copiedSize, newStream.SizeInBytes);

                var curIndex = 0;
                var curTuple = 0;
                foreach (var tuple in allocatedMemory)
                {
                    curTuple++;
                    for (var i = 0; i < tuple.Item2; i++)
                    {
                        Assert.Equal(*(buffer + curIndex), *((byte*) (tuple.Item1 + i)));
                        curIndex++;
                    }

                    unmanagedByteArrayPool.ReturnMemory((byte*) tuple.Item1, tuple.Item2);
                }
            }
        }

        [Fact]
        public void BulkWriteDescendingSizeTest()
        {
            using (var unmanagedByteArrayPool = new UnmanagedByteArrayPool())
            {
                List<Tuple<long, int>> allocatedMemory = new List<Tuple<long, int>>();
                var newStream = new UnmanagedStream(unmanagedByteArrayPool, "trolo", 64, 8);
                var totalSize = 0;
                var rand = new Random();
                var curSize = 0;
                for (var i = 5000; i > 1; i--)
                {
                    var pointer = unmanagedByteArrayPool.GetMemory(rand.Next(1, i * 7), string.Empty, out curSize);
                    totalSize += curSize;
                    FillData(pointer, curSize);
                    allocatedMemory.Add(Tuple.Create((long)pointer, curSize));
                    newStream.Write(pointer, curSize);
                }

                var buffer = unmanagedByteArrayPool.GetMemory(newStream.SizeInBytes, string.Empty, out curSize);

                var copiedSize = newStream.CopyTo(buffer);
                Assert.Equal(copiedSize, newStream.SizeInBytes);

                var curIndex = 0;
                var curTuple = 0;
                foreach (var tuple in allocatedMemory)
                {
                    curTuple++;
                    for (var i = 0; i < tuple.Item2; i++)
                    {
                        Assert.Equal(*(buffer + curIndex), *((byte*)(tuple.Item1 + i)));
                        curIndex++;
                    }

                    unmanagedByteArrayPool.ReturnMemory((byte*)tuple.Item1, tuple.Item2);
                }
            }
        }

        [Fact]
        public void SingleByteWritesTest()
        {
            using (var unmanagedByteArrayPool = new UnmanagedByteArrayPool())
            {
                List<Tuple<long, int>> allocatedMemory = new List<Tuple<long, int>>();
                var newStream = new UnmanagedStream(unmanagedByteArrayPool, "trolo", 64, 8);
                var totalSize = 0;
                var rand = new Random();
                var curSize = 0;
                for (var i = 1; i < 5000; i++)
                {
                    var pointer = unmanagedByteArrayPool.GetMemory(rand.Next(1, i*7), string.Empty, out curSize);
                    totalSize += curSize;
                    FillData(pointer, curSize);
                    allocatedMemory.Add(Tuple.Create((long) pointer, curSize));
                    for (var j = 0; j < curSize; j++)
                    {
                        newStream.WriteByte(*(byte*) (pointer + j));
                    }
                }

                var buffer = unmanagedByteArrayPool.GetMemory(newStream.SizeInBytes, string.Empty, out curSize);

                try
                {
                    var copiedSize = newStream.CopyTo(buffer);
                    Assert.Equal(copiedSize, newStream.SizeInBytes);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                }
                var curIndex = 0;
                var curTuple = 0;
                foreach (var tuple in allocatedMemory)
                {
                    curTuple++;
                    for (var i = 0; i < tuple.Item2; i++)
                    {
                        try
                        {
                            Assert.Equal(*(buffer + curIndex), *((byte*) (tuple.Item1 + i)));
                            curIndex++;
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }
                    }

                    unmanagedByteArrayPool.ReturnMemory((byte*) tuple.Item1, tuple.Item2);
                }
            }
        }

        private void FillData(byte* ptr, int size)
        {
            for (var i = 0; i < size; i++)
            {
                *ptr = (byte) (i%4);
                ptr++;
            }
        }
    }
}