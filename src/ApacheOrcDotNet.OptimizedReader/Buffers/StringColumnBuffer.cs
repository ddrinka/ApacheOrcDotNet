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

        private bool[] _presentStreamValues;
        private long[] _dataStreamValues;
        private long[] _lengthStreamValues;

        private ColumnEncodingKind _encodingKind;
        private int _stripeId;

        public StringColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamValues = new bool[_context.MaxValuesToRead];
            _dataStreamValues = new long[_context.MaxValuesToRead];
            _lengthStreamValues = new long[_context.MaxValuesToRead];
        }

        public override async Task LoadDataAsync(int stripeId, IEnumerable<StreamDetails> columnStreams, RowIndexEntry rowIndexEntry)
        {
            _stripeId = stripeId;
            _encodingKind = columnStreams.First().EncodingKind;

            switch (_encodingKind)
            {
                case ColumnEncodingKind.DirectV2:
                    {
                        LoadStreams(columnStreams, rowIndexEntry, StreamKind.Length, StreamKind.Data);

                        _ = await Task.WhenAll(
                            GetByteRange(_presentStreamCompressedBuffer, _presentStream, _presentStreamPositions),
                            GetByteRange(_lengthStreamCompressedBuffer, _lengthStream, _lengthStreamPositions),
                            GetByteRange(_dataStreamCompressedBuffer, _dataStream, _dataStreamPositions)
                        );

                        DecompressByteRange(_presentStreamCompressedBuffer, _presentStreamDecompressedBuffer, _presentStream, _presentStreamPositions, ref _presentStreamDecompressedBufferLength);
                        DecompressByteRange(_lengthStreamCompressedBuffer, _lengthStreamDecompressedBuffer, _lengthStream, _lengthStreamPositions, ref _lengthStreamDecompressedBufferLength);
                        DecompressByteRange(_dataStreamCompressedBuffer, _dataStreamDecompressedBuffer, _dataStream, _dataStreamPositions, ref _dataStreamDecompressedBufferLength);
                    }
                    break;
                case ColumnEncodingKind.DictionaryV2:
                    {
                        LoadStreams(columnStreams, rowIndexEntry, StreamKind.Length, StreamKind.Data, StreamKind.DictionaryData);

                        _ = await Task.WhenAll(
                            GetByteRange(_presentStreamCompressedBuffer, _presentStream, _presentStreamPositions),
                            GetByteRange(_lengthStreamCompressedBuffer, _lengthStream, _lengthStreamPositions),
                            GetByteRange(_dataStreamCompressedBuffer, _dataStream, _dataStreamPositions),
                            GetByteRange(_dictionaryStreanCompressedBuffer, _dictionaryStream, _dictionaryStreamPositions)
                        );

                        DecompressByteRange(_presentStreamCompressedBuffer, _presentStreamDecompressedBuffer, _presentStream, _presentStreamPositions, ref _presentStreamDecompressedBufferLength);
                        DecompressByteRange(_lengthStreamCompressedBuffer, _lengthStreamDecompressedBuffer, _lengthStream, _lengthStreamPositions, ref _lengthStreamDecompressedBufferLength);
                        DecompressByteRange(_dataStreamCompressedBuffer, _dataStreamDecompressedBuffer, _dataStream, _dataStreamPositions, ref _dataStreamDecompressedBufferLength);
                        DecompressByteRange(_dictionaryStreanCompressedBuffer, _dictionaryStreamDecompressedBuffer, _dictionaryStream, _dictionaryStreamPositions, ref _dictionaryStreamDecompressedBufferLength);
                    }
                    break;
            }
        }

        public override void Parse()
        {
            switch (_encodingKind)
            {
                case ColumnEncodingKind.DirectV2:
                    ReadDirectV2();
                    break;
                case ColumnEncodingKind.DictionaryV2:
                    ReadDictionaryV2();
                    break;
                default:
                    throw new InvalidOperationException();
            }
        }

        private void ReadDirectV2()
        {
            ReadBooleanStream(_presentStreamDecompressedBuffer, _presentStreamDecompressedBufferLength, _presentStreamPositions, _presentStreamValues, out var presentValuesRead);
            ReadNumericStream(_lengthStreamDecompressedBuffer, _lengthStreamDecompressedBufferLength, _lengthStreamPositions, isSigned: false, _lengthStreamValues, out var lengthValuesRead);

            var dataBuffer = ResizeBuffer(_dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength, _dataStreamPositions);

            var stringOffset = 0;
            if (_presentStream != null)
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

                    if (_numValuesRead >= _values.Length)
                        break;
                }
            }
            else
            {
                for (int idx = 0; idx < lengthValuesRead; idx++)
                {
                    var length = (int)_lengthStreamValues[idx];
                    _values[_numValuesRead++] = Encoding.UTF8.GetString(dataBuffer.Slice(stringOffset, length));
                    stringOffset += length;

                    if (_numValuesRead >= _values.Length)
                        break;
                }
            }
        }

        private void ReadDictionaryV2()
        {
            var dictionaryV2LengthStreamBuffer = GetLengthStreamBufferDictinaryV2(_stripeId, _lengthStream.DictionarySize);

            ReadBooleanStream(_presentStreamDecompressedBuffer, _presentStreamDecompressedBufferLength, _presentStreamPositions, _presentStreamValues, out var presentValuesRead);
            ReadNumericStream(_lengthStreamDecompressedBuffer, _lengthStreamDecompressedBufferLength, _lengthStreamPositions, isSigned: false, dictionaryV2LengthStreamBuffer, out var lengthValuesRead);
            ReadNumericStream(_dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength, _dataStreamPositions, isSigned: false, _dataStreamValues, out var dataValuesRead);

            int stringOffset = 0;
            List<string> stringsList = new(lengthValuesRead);
            for (int idx = 0; idx < lengthValuesRead; idx++)
            {
                var length = (int)dictionaryV2LengthStreamBuffer[idx];
                var value = Encoding.UTF8.GetString(_dictionaryStreamDecompressedBuffer.AsSpan().Slice(stringOffset, length));
                stringOffset += length;
                stringsList.Add(value);
            }

            if (_presentStream != null)
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

        private long[] GetLengthStreamBufferDictinaryV2(int stripeId, int dictionarySize)
        {
            if (!_dictionaryLengthBuffers.ContainsKey(stripeId))
                _dictionaryLengthBuffers.Add(stripeId, new long[dictionarySize]);

            return _dictionaryLengthBuffers[stripeId];
        }
    }
}
