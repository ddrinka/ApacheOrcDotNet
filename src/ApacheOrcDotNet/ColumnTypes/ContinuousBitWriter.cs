using ApacheOrcDotNet.Encodings;
using System.Collections.Generic;
using System.IO;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class ContinuousBitWriter
    {
        readonly ByteRunLengthEncodingWriter _byteWriter;
        private byte byteBuffer;
        private int bitIndex;
        private bool hasData;

        public ContinuousBitWriter(Stream outputStream)
        {
            _byteWriter = new ByteRunLengthEncodingWriter(outputStream);
            Flush();
        }

        public void Write(IList<bool> values)
        {
            foreach (var value in values)
            {
                Write(value);
            }
        }

        public void Write(bool value)
        {
            if (value)
                byteBuffer |= (byte)(1 << bitIndex);

            hasData = true;
            bitIndex--;

            if (bitIndex == -1)
            {
                Flush();
            }
        }

        public void Flush()
        {
            if (hasData)
            {
                _byteWriter.Write(new[] { byteBuffer });
            }

            byteBuffer = 0;
            bitIndex = 7;
            hasData = false;
        }
    }
}
