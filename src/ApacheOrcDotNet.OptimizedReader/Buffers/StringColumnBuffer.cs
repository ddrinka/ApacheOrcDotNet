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

        public StringColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
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

            // Positions
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var lengthPositions = GetTargetedStreamPositions(presentStream, lengthStream, rowIndexEntry);
            var dataPositions = GetTargetedStreamPositions(presentStream, dataStream, rowIndexEntry);

            // Decompression
            var presentMemory = _byteRangeProvider.DecompressByteRangeNew(_context, presentStream, in presentPositions).Sequence;
            var lengthMemory = _byteRangeProvider.DecompressByteRangeNew(_context, lengthStream, in lengthPositions).Sequence;
            var dataMemory = _byteRangeProvider.DecompressByteRangeNew(_context, dataStream, in dataPositions).Sequence;

            // Processing
            var numPresentValuesRead = ReadBooleanStream(in presentMemory, presentPositions, _presentStreamBuffer);
            var numLengthValuesRead = ReadNumericStream(in lengthMemory, lengthPositions, isSigned: false, _lengthStreamBufferDirectV2);

            var rowEntryLength = dataMemory.Length - dataPositions.RowEntryOffset;
            var dataSequence = dataMemory.Slice(dataPositions.RowEntryOffset, rowEntryLength);

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

        [SkipLocalsInit]
        private void ReadDictionaryV2(IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry, int stripeId)
        {
            var presentStream = GetStripeStream(columnStreams, StreamKind.Present, isRequired: false);
            var lengthStream = GetStripeStream(columnStreams, StreamKind.Length);
            var dataStream = GetStripeStream(columnStreams, StreamKind.Data);
            var dictionaryDataStream = GetStripeStream(columnStreams, StreamKind.DictionaryData);

            // Positions
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var lengthPositions = GetTargetedStreamPositions(presentStream, lengthStream, rowIndexEntry);
            var dataPositions = GetTargetedStreamPositions(presentStream, dataStream, rowIndexEntry);
            var dictionaryDataPositions = GetTargetedStreamPositions(presentStream, dictionaryDataStream, rowIndexEntry);

            // Decompression
            var presentMemory = _byteRangeProvider.DecompressByteRangeNew(_context, presentStream, in presentPositions).Sequence;
            var lengthMemory = _byteRangeProvider.DecompressByteRangeNew(_context, lengthStream, in lengthPositions).Sequence;
            var dataMemory = _byteRangeProvider.DecompressByteRangeNew(_context, dataStream, in dataPositions).Sequence;
            var dictionaryDataMemory = _byteRangeProvider.DecompressByteRangeNew(_context, dictionaryDataStream, in dictionaryDataPositions).Sequence;

            // Processing
            var lengthStreamBuffer = GetLengthStreamBufferDictinaryV2(stripeId, lengthStream.DictionarySize);
            var numPresentValuesRead = ReadBooleanStream(in presentMemory, in presentPositions, _presentStreamBuffer);
            var numLengthValuesRead = ReadNumericStream(in lengthMemory, in lengthPositions, isSigned: false, lengthStreamBuffer);
            var numDataValuesRead = ReadNumericStream(in dataMemory, in dataPositions, isSigned: false, _dataStreamBuffer);

            int stringOffset = 0;
            List<string> stringsList = new(numLengthValuesRead);
            for (int idx = 0; idx < numLengthValuesRead; idx++)
            {
                var length = (int)lengthStreamBuffer[idx];
                var value = Encoding.UTF8.GetString(dictionaryDataMemory.Slice(stringOffset, length));
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

        private Span<long> GetLengthStreamBufferDictinaryV2(int stripeId, int dictionarySize)
        {
            if (!_dictionaryLengthBuffers.ContainsKey(stripeId))
                _dictionaryLengthBuffers.Add(stripeId, new long[dictionarySize]);

            return _dictionaryLengthBuffers[stripeId];
        }
    }
}
