﻿using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public class StringColumnBuffer : BaseColumnBuffer<string>
    {
        private readonly Dictionary<int, long[]> _dictionaryLengthBuffers = new();
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

        private ColumnEncodingKind _encodingKind;
        private ColumnDataStreams _streams;
        private int _stripeId;

        public StringColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamValues = new bool[_context.MaxValuesToRead];
            _dataStreamValues = new long[_context.MaxValuesToRead];
            _lengthStreamValues = new long[_context.MaxValuesToRead];

            _dataStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _dataStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength);

            _dictionaryStreanCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _dictionaryStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength);

            _lengthStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _lengthStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength);

            _presentStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _presentStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength);
        }

        public override async Task LoadDataAsync(int stripeId, ColumnDataStreams streams)
        {
            _streams = streams;
            _stripeId = stripeId;
            _encodingKind = _streams.EncodingKind;

            var byteRangeTasks = new List<Task<int>>()
            {
                GetByteRangeAsync(_streams.Present, _presentStreamCompressedBuffer),
                GetByteRangeAsync(_streams.Length, _lengthStreamCompressedBuffer),
                GetByteRangeAsync(_streams.Data, _dataStreamCompressedBuffer)
            };
            if (_encodingKind == ColumnEncodingKind.DictionaryV2)
                byteRangeTasks.Add(GetByteRangeAsync(_streams.DictionaryData, _dictionaryStreanCompressedBuffer));

            _ = await Task.WhenAll(byteRangeTasks);

            DecompressByteRange(_streams.Present, _presentStreamCompressedBuffer, _presentStreamDecompressedBuffer, ref _presentStreamDecompressedBufferLength);
            DecompressByteRange(_streams.Length, _lengthStreamCompressedBuffer, _lengthStreamDecompressedBuffer, ref _lengthStreamDecompressedBufferLength);
            DecompressByteRange(_streams.Data, _dataStreamCompressedBuffer, _dataStreamDecompressedBuffer, ref _dataStreamDecompressedBufferLength);
            if (_encodingKind == ColumnEncodingKind.DictionaryV2)
                DecompressByteRange(_streams.DictionaryData, _dictionaryStreanCompressedBuffer, _dictionaryStreamDecompressedBuffer, ref _dictionaryStreamDecompressedBufferLength);
        }

        public override void Fill()
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
            ReadBooleanStream(_streams.Present, _presentStreamDecompressedBuffer, _presentStreamDecompressedBufferLength, _presentStreamValues, out var presentValuesRead);
            ReadNumericStream(_streams.Length, _lengthStreamDecompressedBuffer, _lengthStreamDecompressedBufferLength, isSigned: false, _lengthStreamValues, out var lengthValuesRead);

            var dataBuffer = ResizeBuffer(_streams.Data, _dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength);

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
            var dictionaryV2LengthStreamBuffer = GetLengthStreamBufferDictinaryV2(_stripeId, _streams.Length.DictionarySize);

            ReadBooleanStream(_streams.Present, _presentStreamDecompressedBuffer, _presentStreamDecompressedBufferLength, _presentStreamValues, out var presentValuesRead);
            ReadNumericStream(_streams.Length, _lengthStreamDecompressedBuffer, _lengthStreamDecompressedBufferLength, isSigned: false, dictionaryV2LengthStreamBuffer, out var lengthValuesRead);
            ReadNumericStream(_streams.Data, _dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength, isSigned: false, _dataStreamValues, out var dataValuesRead);

            int stringOffset = 0;
            List<string> stringsList = new(lengthValuesRead);
            for (int idx = 0; idx < lengthValuesRead; idx++)
            {
                var length = (int)dictionaryV2LengthStreamBuffer[idx];
                var value = Encoding.UTF8.GetString(_dictionaryStreamDecompressedBuffer.AsSpan().Slice(stringOffset, length));
                stringOffset += length;
                stringsList.Add(value);
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

        private long[] GetLengthStreamBufferDictinaryV2(int stripeId, int dictionarySize)
        {
            if (!_dictionaryLengthBuffers.ContainsKey(stripeId))
                _dictionaryLengthBuffers.Add(stripeId, new long[dictionarySize]);

            return _dictionaryLengthBuffers[stripeId];
        }
    }
}
