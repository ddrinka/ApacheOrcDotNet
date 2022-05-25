using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class StringColumnBuffer : BaseColumnBuffer<string>
    {
        private readonly Dictionary<int, long[]> _dictionaryLengthBuffers = new();

        private bool[] _presentStreamBuffer;
        private long[] _dataStreamBuffer;
        private long[] _lengthStreamBufferDirectV2;

        public StringColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContextNew context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
            _dataStreamBuffer = new long[_context.MaxValuesToRead];
            _lengthStreamBufferDirectV2 = new long[_context.MaxValuesToRead];
        }

        public override void Fill(int stripeId, IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry)
        {
            ResetInnerBuffers();

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
            var presentStream = GetStripeStream(columnStreams, StreamKind.Present, isRequired: false);
            var lengthStream = GetStripeStream(columnStreams, StreamKind.Length);
            var dataStream = GetStripeStream(columnStreams, StreamKind.Data);

            // Present
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var numPresentValuesRead = ReadBooleanStream(presentStream, presentPositions, _presentStreamBuffer);

            // Length
            var lengthPositions = GetTargetedStreamPositions(presentStream, lengthStream, rowIndexEntry);
            var numLengthValuesRead = ReadNumericStream(lengthStream, lengthPositions, isSigned: false, _lengthStreamBufferDirectV2);

            // Data
            var dataStreamPostions = GetTargetedStreamPositions(presentStream, dataStream, rowIndexEntry);
            var dataBuffer = _byteRangeProvider.DecompressByteRange(
                offset: dataStream.FileOffset + dataStreamPostions.RowGroupOffset,
                compressedLength: dataStream.Length - dataStreamPostions.RowGroupOffset,
                compressionKind: _context.CompressionKind,
                compressionBlockSize: _context.CompressionBlockSize
            );

            using (dataBuffer)
            {
                var rowEntryLength = dataBuffer.Sequence.Length - dataStreamPostions.RowEntryOffset;
                var dataSequence = dataBuffer.Sequence.Slice(dataStreamPostions.RowEntryOffset, rowEntryLength);

                var stringOffset = 0;
                if (presentStream != null)
                {
                    var lengthIndex = 0;
                    for (int idx = 0; idx < numPresentValuesRead; idx++)
                    {
                        if (_presentStreamBuffer[idx])
                        {
                            var length = (int)_lengthStreamBufferDirectV2[lengthIndex++];
                            _values[_numValuesRead++] = Encoding.UTF8.GetString(dataSequence.Slice(stringOffset, length));
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
                    for (int idx = 0; idx < numLengthValuesRead; idx++)
                    {
                        var length = (int)_lengthStreamBufferDirectV2[idx];
                        _values[_numValuesRead++] = Encoding.UTF8.GetString(dataSequence.Slice(stringOffset, length));
                        stringOffset += length;

                        if (_numValuesRead >= _values.Length)
                            break;
                    }
                }
            }
        }

        [SkipLocalsInit]
        private void ReadDictionaryV2(IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry, int stripeId)
        {
            var presentStream = GetStripeStream(columnStreams, StreamKind.Present, isRequired: false);
            var lengthStream = GetStripeStream(columnStreams, StreamKind.Length);
            var dataStream = GetStripeStream(columnStreams, StreamKind.Data);
            var dictionaryDataStream = GetStripeStream(columnStreams, StreamKind.DictionaryData);

            // Present
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var numPresentValuesRead = ReadBooleanStream(presentStream, in presentPositions, _presentStreamBuffer);

            // Length
            var lengthPositions = GetTargetedStreamPositions(presentStream, lengthStream, rowIndexEntry);
            var lengthStreamBuffer = GetLengthStreamBufferDictinaryV2(stripeId, lengthStream.DictionarySize);
            var numLengthValuesRead = ReadNumericStream(lengthStream, in lengthPositions, isSigned: false, lengthStreamBuffer);

            // Data
            var dataPostions = GetTargetedStreamPositions(presentStream, dataStream, rowIndexEntry);
            var numDataValuesRead = ReadNumericStream(dataStream, in dataPostions, isSigned: false, _dataStreamBuffer);

            // Dictionary Data
            var dictionaryDataPositions = GetTargetedStreamPositions(presentStream, dictionaryDataStream, rowIndexEntry);
            var decompressedBuffer = _byteRangeProvider.DecompressByteRange(
                offset: dictionaryDataStream.FileOffset + dictionaryDataPositions.RowGroupOffset,
                compressedLength: dictionaryDataStream.Length - dictionaryDataPositions.RowGroupOffset,
                compressionKind: _context.CompressionKind,
                compressionBlockSize: _context.CompressionBlockSize
            );

            using (decompressedBuffer)
            {
                int stringOffset = 0;
                List<string> stringsList = new(numLengthValuesRead);
                for (int idx = 0; idx < numLengthValuesRead; idx++)
                {
                    var length = (int)lengthStreamBuffer[idx];
                    var value = Encoding.UTF8.GetString(decompressedBuffer.Sequence.Slice(stringOffset, length));
                    stringOffset += length;
                    stringsList.Add(value);
                }

                if (presentStream != null)
                {
                    var dataIndex = 0;
                    for (int idx = 0; idx < numPresentValuesRead; idx++)
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
                    for (int idx = 0; idx < numDataValuesRead; idx++)
                    {
                        _values[_numValuesRead++] = stringsList[(int)_dataStreamBuffer[idx]];

                        if (_numValuesRead >= _values.Length)
                            break;
                    }
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
