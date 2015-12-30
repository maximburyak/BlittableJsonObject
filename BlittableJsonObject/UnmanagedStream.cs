using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Sparrow;

namespace NewBlittable
{
    public unsafe class UnmanagedStream : IDisposable
    {
        private readonly UnmanagedBuffersPool _buffersPool;
        private readonly string _documentId;
        private readonly int _initialSize;

        readonly List<ulong> _segments;

        private byte* _curSegmentAddress;
        private int _curSegmentSize;
        private int _positionInCurSegment;

        private int _sizeInBytes;
        private bool _disposed;

        public int SizeInBytes => _sizeInBytes;

        public UnmanagedStream(UnmanagedBuffersPool buffersPool, string documentId, int initialSize = 64, int initialSegmentsAmount = 8)
        {

            _buffersPool = buffersPool;
            _documentId = documentId;
            _segments = new List<ulong>(initialSegmentsAmount);
            _positionInCurSegment = 0;

            _curSegmentAddress = _buffersPool.GetMemory(initialSize, documentId, out _curSegmentSize);
            _initialSize = _curSegmentSize;

            _segments.Add((ulong)_curSegmentAddress);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetNextPowerOfTwo(int number)
        {
            return (int)Math.Pow(2, Math.Ceiling(Math.Log(number, 2)));
        }

        public void Write(byte* buffer, int length)
        {
            var bufferPosition = 0;
            var lengthLeft = length;
            do
            {
                // Create next, bigger segment if needed
                if (_positionInCurSegment == _curSegmentSize)
                {
                    var nextSegmentSize = GetNextPowerOfTwo(_curSegmentSize + 1);
                    while (nextSegmentSize <= lengthLeft)
                    {
                        nextSegmentSize = GetNextPowerOfTwo(nextSegmentSize + 1);
                        _segments.Add(0);
                    }

                    _curSegmentAddress = _buffersPool.GetMemory(nextSegmentSize, _documentId, out _curSegmentSize);
                    _positionInCurSegment = 0;
                    _segments.Add((ulong)_curSegmentAddress);
                }

                // write data or part of it to segment until it's end
                if (lengthLeft > _curSegmentSize - _positionInCurSegment)
                {
                    Memory.Copy(_curSegmentAddress + _positionInCurSegment, buffer, _curSegmentSize - _positionInCurSegment);
                    _sizeInBytes += _curSegmentSize - _positionInCurSegment;
                    lengthLeft -= _curSegmentSize - _positionInCurSegment;
                    bufferPosition += _curSegmentSize - _positionInCurSegment;
                    buffer += _curSegmentSize - _positionInCurSegment;
                    _positionInCurSegment += _curSegmentSize - _positionInCurSegment;
                }
                else
                {
                    Memory.Copy(_curSegmentAddress + _positionInCurSegment, buffer, lengthLeft);
                    bufferPosition += lengthLeft;
                    _positionInCurSegment += lengthLeft;
                    _sizeInBytes += lengthLeft;
                    lengthLeft = 0;
                }

            } while (bufferPosition < length);
        }

        public void WriteByte(byte data)
        {
            if (_positionInCurSegment == _curSegmentSize)
            {
                _curSegmentAddress = _buffersPool.GetMemory(_curSegmentSize + 1, _documentId, out _curSegmentSize);
                _positionInCurSegment = 0;
                _segments.Add((ulong)_curSegmentAddress);
            }
            _sizeInBytes++;

            *(byte*)(_curSegmentAddress + _positionInCurSegment) = data;
            _positionInCurSegment++;
        }

        public int CopyTo(byte* pointer)
        {
            var curSize = _initialSize;
            var copiedBytes = 0;

            for (int i = 0; i < _segments.Count - 1; i++)
            {
                if (_segments[i] == 0)
                {
                    curSize *= 2;
                    continue;
                }

                Memory.Copy(pointer, (byte*)_segments[i], curSize);
                pointer += curSize;
                copiedBytes += curSize;
                curSize *= 2;
            }

            Memory.Copy(pointer, (byte*)_curSegmentAddress, _positionInCurSegment);
            copiedBytes += _positionInCurSegment;

            return copiedBytes;
        }


        public void Dispose()
        {
            lock (_segments)
            {
                if (_disposed == false)
                {
                    _disposed = true;
                    var curSize = _initialSize;
                    for (int i = 0; i < _segments.Count - 1; i++)
                    {
                        if (_segments[i] == 0)
                            continue;
                        _buffersPool.ReturnMemory((byte*)_segments[i]);
                        curSize *= 2;
                    }
                }
            }
        }
    }
}
