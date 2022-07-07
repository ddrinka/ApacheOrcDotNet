using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class StringColumnBuffer : BaseColumnBuffer<string>
    {
        private readonly Dictionary<int, List<string>> _stripeDictionaries = new();
        private readonly bool[] _presentStreamValues;
        private readonly long[] _dataStreamValues;
        private readonly long[] _lengthStreamValues;

        private byte[] _dataStreamCompressedBuffer;
        private byte[] _dataStreamDecompressedBuffer;
        private int _dataStreamDecompressedBufferLength;

        private byte[] _dictionaryStreanCompressedBuffer;
        private byte[] _dictionaryStreamDecompressedBuffer;
        private int _dictionaryStreamDecompressedBufferLength;

        private byte[] _lengthStreamCompressedBuffer;
        private byte[] _lengthStreamDecompressedBuffer;
        private int _lengthStreamDecompressedBufferLength;

        private byte[] _presentStreamCompressedBuffer;
        private byte[] _presentStreamDecompressedBuffer;
        private int _presentStreamDecompressedBufferLength;

        public StringColumnBuffer(IByteRangeProvider byteRangeProvider, OrcFileProperties orcFileProperties, OrcColumn column) : base(byteRangeProvider, orcFileProperties, column)
        {
            _presentStreamValues = new bool[_orcFileProperties.MaxValuesToRead];
            _dataStreamValues = new long[_orcFileProperties.MaxValuesToRead];
            _lengthStreamValues = new long[_orcFileProperties.MaxValuesToRead];

            _dataStreamCompressedBuffer = new byte[_orcFileProperties.MaxCompressedBufferLength];
            _dataStreamDecompressedBuffer = new byte[_orcFileProperties.MaxDecompressedBufferLength];

            _dictionaryStreanCompressedBuffer = new byte[_orcFileProperties.MaxCompressedBufferLength];
            _dictionaryStreamDecompressedBuffer = new byte[_orcFileProperties.MaxDecompressedBufferLength];

            _lengthStreamCompressedBuffer = new byte[_orcFileProperties.MaxCompressedBufferLength];
            _lengthStreamDecompressedBuffer = new byte[_orcFileProperties.MaxDecompressedBufferLength];

            _presentStreamCompressedBuffer = new byte[_orcFileProperties.MaxCompressedBufferLength];
            _presentStreamDecompressedBuffer = new byte[_orcFileProperties.MaxDecompressedBufferLength];
        }

        public override async Task LoadDataAsync(int stripeId, ColumnDataStreams streams)
        {
            var byteRangeTasks = new List<Task<int>>()
            {
                GetByteRangeAsync(streams.Present, _presentStreamCompressedBuffer),
                GetByteRangeAsync(streams.Length, _lengthStreamCompressedBuffer),
                GetByteRangeAsync(streams.Data, _dataStreamCompressedBuffer)
            };
            if (streams.EncodingKind == ColumnEncodingKind.DictionaryV2)
                byteRangeTasks.Add(GetByteRangeAsync(streams.DictionaryData, _dictionaryStreanCompressedBuffer));

            _ = await Task.WhenAll(byteRangeTasks);

            DecompressByteRange(streams.Present, _presentStreamCompressedBuffer, _presentStreamDecompressedBuffer, ref _presentStreamDecompressedBufferLength);
            DecompressByteRange(streams.Length, _lengthStreamCompressedBuffer, _lengthStreamDecompressedBuffer, ref _lengthStreamDecompressedBufferLength);
            DecompressByteRange(streams.Data, _dataStreamCompressedBuffer, _dataStreamDecompressedBuffer, ref _dataStreamDecompressedBufferLength);
            if (streams.EncodingKind == ColumnEncodingKind.DictionaryV2)
                DecompressByteRange(streams.DictionaryData, _dictionaryStreanCompressedBuffer, _dictionaryStreamDecompressedBuffer, ref _dictionaryStreamDecompressedBufferLength);

            Fill(stripeId, streams);
        }

        private void Fill(int stripeId, ColumnDataStreams streams)
        {
            switch (streams.EncodingKind)
            {
                case ColumnEncodingKind.DirectV2:
                    ReadDirectV2(streams);
                    break;
                case ColumnEncodingKind.DictionaryV2:
                    ReadDictionaryV2(stripeId, streams);
                    break;
                default:
                    throw new InvalidOperationException();
            }
        }

        private void ReadDirectV2(ColumnDataStreams streams)
        {
            ReadBooleanStream(streams.Present, _presentStreamDecompressedBuffer, _presentStreamDecompressedBufferLength, _presentStreamValues, out var presentValuesRead);
            ReadNumericStream(streams.Length, _lengthStreamDecompressedBuffer, _lengthStreamDecompressedBufferLength, isSigned: false, _lengthStreamValues, out var lengthValuesRead);

            var dataBuffer = GetDataStream(streams.Data, _dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength);

            var stringOffset = 0;
            if (presentValuesRead > 0)
            {
                var lengthIndex = 0;
                for (int idx = 0; idx < presentValuesRead; idx++)
                {
                    if (_presentStreamValues[idx])
                    {
                        var length = (int)_lengthStreamValues[lengthIndex++];
                        _values[_numValuesRead++] = Encoding.UTF8.GetString(dataBuffer.Slice(stringOffset, length));
                        stringOffset += length;
                    }
                    else
                        _values[_numValuesRead++] = null;
                }
            }
            else
            {
                for (int idx = 0; idx < lengthValuesRead; idx++)
                {
                    var length = (int)_lengthStreamValues[idx];
                    _values[_numValuesRead++] = Encoding.UTF8.GetString(dataBuffer.Slice(stringOffset, length));
                    stringOffset += length;
                }
            }
        }

        private void ReadDictionaryV2(int stripeId, ColumnDataStreams streams)
        {
            ReadBooleanStream(streams.Present, _presentStreamDecompressedBuffer, _presentStreamDecompressedBufferLength, _presentStreamValues, out var presentValuesRead);
            ReadNumericStream(streams.Data, _dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength, isSigned: false, _dataStreamValues, out var dataValuesRead);

            if (!_stripeDictionaries.TryGetValue(stripeId, out var stringsList))
            {
                Span<long> dictionaryV2LengthStreamBuffer = stackalloc long[streams.Length.DictionarySize];
                ReadNumericStream(streams.Length, _lengthStreamDecompressedBuffer, _lengthStreamDecompressedBufferLength, isSigned: false, dictionaryV2LengthStreamBuffer, out var lengthValuesRead);

                int stringOffset = 0;
                stringsList = new List<string>();
                for (int idx = 0; idx < lengthValuesRead; idx++)
                {
                    var length = (int)dictionaryV2LengthStreamBuffer[idx];
                    var value = Encoding.UTF8.GetString(_dictionaryStreamDecompressedBuffer.AsSpan().Slice(stringOffset, length));
                    stringOffset += length;
                    stringsList.Add(value);
                }

                _stripeDictionaries[stripeId] = stringsList;
            }

            if (presentValuesRead > 0)
            {
                var dataIndex = 0;
                for (int idx = 0; idx < presentValuesRead; idx++)
                {
                    if (_presentStreamValues[idx])
                        _values[_numValuesRead++] = stringsList[(int)_dataStreamValues[dataIndex++]];
                    else
                        _values[_numValuesRead++] = null;

                    if (_numValuesRead >= _values.Length)
                        break;
                }
            }
            else
            {
                for (int idx = 0; idx < dataValuesRead; idx++)
                {
                    _values[_numValuesRead++] = stringsList[(int)_dataStreamValues[idx]];

                    if (_numValuesRead >= _values.Length)
                        break;
                }
            }
        }
    }
}
