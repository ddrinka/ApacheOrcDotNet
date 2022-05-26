using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public class StringColumnBuffer : BaseColumnBuffer<string>
    {
        private readonly Dictionary<int, long[]> _dictionaryLengthBuffers = new();

        private bool[] _presentStreamBuffer;
        private long[] _dataStreamBuffer;
        private long[] _lengthStreamBufferDirectV2;
        private byte[] _presentInputBuffer;
        private byte[] _presentOutputBuffer;
        private byte[] _lengthInputBuffer;
        private byte[] _lengthOutputBuffer;
        private byte[] _dataInputBuffer;
        private byte[] _dataOutputBuffer;
        private byte[] _dictionaryInputBuffer;
        private byte[] _dictionaryOutputBuffer;

        public StringColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
            _dataStreamBuffer = new long[_context.MaxValuesToRead];
            _lengthStreamBufferDirectV2 = new long[_context.MaxValuesToRead];

            _presentInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _presentOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);

            _lengthInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _lengthOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);

            _dataInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _dataOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);

            _dictionaryInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _dictionaryOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);
        }

        public override void Fill(int stripeId, IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry)
        {
            switch (columnStreams.First().EncodingKind)
            {
                case ColumnEncodingKind.DirectV2:
                    ReadDirectV2(columnStreams, rowIndexEntry);
                    break;
                case ColumnEncodingKind.DictionaryV2:
                    ReadDictionaryV2(columnStreams, rowIndexEntry, stripeId);
                    break;
                default:
                    throw new InvalidOperationException();
            }
        }

        private void ReadDirectV2(IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry)
        {
            // Streams
            var presentStream = GetColumnStream(columnStreams, StreamKind.Present, isRequired: false);
            var lengthStream = GetColumnStream(columnStreams, StreamKind.Length);
            var dataStream = GetColumnStream(columnStreams, StreamKind.Data);

            // Stream Positions
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var lengthPositions = GetTargetDataStreamPositions(presentStream, lengthStream, rowIndexEntry);
            var dataPositions = GetTargetDataStreamPositions(presentStream, dataStream, rowIndexEntry);

            // Stream Byte Ranges
            (int present, int length, int data) rangeSizes = default;
            Parallel.Invoke(
                () => GetByteRange(_presentInputBuffer, presentStream, presentPositions, ref rangeSizes.present),
                () => GetByteRange(_lengthInputBuffer, lengthStream, lengthPositions, ref rangeSizes.length),
                () => GetByteRange(_dataInputBuffer, dataStream, dataPositions, ref rangeSizes.data)
            );

            // Decompress Byte Ranges
            (int present, int length, int data) decompressedSizes = default;
            Parallel.Invoke(
                () => DecompressByteRange(_presentInputBuffer, _presentOutputBuffer, presentStream, presentPositions, ref decompressedSizes.present),
                () => DecompressByteRange(_lengthInputBuffer, _lengthOutputBuffer, lengthStream, lengthPositions, ref decompressedSizes.length),
                () => DecompressByteRange(_dataInputBuffer, _dataOutputBuffer, dataStream, dataPositions, ref decompressedSizes.data)
            );

            // Parse Decompressed Bytes
            (int present, int length) valuesRead = default;
            Parallel.Invoke(
                () => ReadBooleanStream(_presentOutputBuffer, decompressedSizes.present, presentPositions, _presentStreamBuffer, ref valuesRead.present),
                () => ReadNumericStream(_lengthOutputBuffer, decompressedSizes.length, lengthPositions, isSigned: false, _lengthStreamBufferDirectV2, ref valuesRead.length)
            );

            var dataBuffer = ResizeBuffer(_dataOutputBuffer, decompressedSizes.data, dataPositions);

            var stringOffset = 0;
            if (presentStream != null)
            {
                var lengthIndex = 0;
                for (int idx = 0; idx < valuesRead.present; idx++)
                {
                    if (_presentStreamBuffer[idx])
                    {
                        var length = (int)_lengthStreamBufferDirectV2[lengthIndex++];
                        _values[_numValuesRead++] = Encoding.UTF8.GetString(dataBuffer.Slice(stringOffset, length));
                        stringOffset += length;
                    }
                    else
                        _values[_numValuesRead++] = null;

                    if (_numValuesRead >= _values.Length)
                        break;
                }
            }
            else
            {
                for (int idx = 0; idx < valuesRead.length; idx++)
                {
                    var length = (int)_lengthStreamBufferDirectV2[idx];
                    _values[_numValuesRead++] = Encoding.UTF8.GetString(dataBuffer.Slice(stringOffset, length));
                    stringOffset += length;

                    if (_numValuesRead >= _values.Length)
                        break;
                }
            }
        }

        private void ReadDictionaryV2(IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry, int stripeId)
        {
            // Streams
            var presentStream = GetColumnStream(columnStreams, StreamKind.Present, isRequired: false);
            var lengthStream = GetColumnStream(columnStreams, StreamKind.Length);
            var dataStream = GetColumnStream(columnStreams, StreamKind.Data);
            var dictionaryDataStream = GetColumnStream(columnStreams, StreamKind.DictionaryData);

            // Stream Positions
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var lengthPositions = GetTargetDataStreamPositions(presentStream, lengthStream, rowIndexEntry);
            var dataPositions = GetTargetDataStreamPositions(presentStream, dataStream, rowIndexEntry);
            var dictionaryDataPositions = GetTargetDataStreamPositions(presentStream, dictionaryDataStream, rowIndexEntry);

            // Stream Byte Ranges
            (int present, int length, int data, int dictionary) rangeSizes = default;
            Parallel.Invoke(
                () => GetByteRange(_presentInputBuffer, presentStream, presentPositions, ref rangeSizes.present),
                () => GetByteRange(_lengthInputBuffer, lengthStream, lengthPositions, ref rangeSizes.length),
                () => GetByteRange(_dataInputBuffer, dataStream, dataPositions, ref rangeSizes.data),
                () => GetByteRange(_dictionaryInputBuffer, dictionaryDataStream, dictionaryDataPositions, ref rangeSizes.dictionary)
            );

            // Decompress Byte Ranges
            (int present, int length, int data, int dictionary) decompressedSizes = default;
            Parallel.Invoke(
                () => DecompressByteRange(_presentInputBuffer, _presentOutputBuffer, presentStream, presentPositions, ref decompressedSizes.present),
                () => DecompressByteRange(_lengthInputBuffer, _lengthOutputBuffer, lengthStream, lengthPositions, ref decompressedSizes.length),
                () => DecompressByteRange(_dataInputBuffer, _dataOutputBuffer, dataStream, dataPositions, ref decompressedSizes.data),
                () => DecompressByteRange(_dictionaryInputBuffer, _dictionaryOutputBuffer, dictionaryDataStream, dictionaryDataPositions, ref decompressedSizes.dictionary)
            );

            // Parse Decompressed Bytes
            (int present, int length, int data) valuesRead = default;
            var dictionaryV2LengthStreamBuffer = GetLengthStreamBufferDictinaryV2(stripeId, lengthStream.DictionarySize);
            ReadNumericStream(_lengthOutputBuffer, decompressedSizes.length, lengthPositions, isSigned: false, dictionaryV2LengthStreamBuffer, ref valuesRead.length);

            Parallel.Invoke(
                () => ReadBooleanStream(_presentOutputBuffer, decompressedSizes.present, presentPositions, _presentStreamBuffer, ref valuesRead.present),
                () => ReadNumericStream(_dataOutputBuffer, decompressedSizes.data, dataPositions, isSigned: false, _dataStreamBuffer, ref valuesRead.data)
            );

            int stringOffset = 0;
            List<string> stringsList = new(valuesRead.length);
            for (int idx = 0; idx < valuesRead.length; idx++)
            {
                var length = (int)dictionaryV2LengthStreamBuffer[idx];
                var value = Encoding.UTF8.GetString(_dictionaryOutputBuffer.AsSpan().Slice(stringOffset, length));
                stringOffset += length;
                stringsList.Add(value);
            }

            if (presentStream != null)
            {
                var dataIndex = 0;
                for (int idx = 0; idx < valuesRead.present; idx++)
                {
                    if (_presentStreamBuffer[idx])
                        _values[_numValuesRead++] = stringsList[(int)_dataStreamBuffer[dataIndex++]];
                    else
                        _values[_numValuesRead++] = null;

                    if (_numValuesRead >= _values.Length)
                        break;
                }
            }
            else
            {
                for (int idx = 0; idx < valuesRead.data; idx++)
                {
                    _values[_numValuesRead++] = stringsList[(int)_dataStreamBuffer[idx]];

                    if (_numValuesRead >= _values.Length)
                        break;
                }
            }
        }

        private Span<long> GetLengthStreamBufferDictinaryV2(int stripeId, int dictionarySize)
        {
            if (!_dictionaryLengthBuffers.ContainsKey(stripeId))
                _dictionaryLengthBuffers.Add(stripeId, new long[dictionarySize]);

            return _dictionaryLengthBuffers[stripeId];
        }
    }
}
