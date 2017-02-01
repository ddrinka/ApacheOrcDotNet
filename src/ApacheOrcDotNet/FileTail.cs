using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Infrastructure;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet
{
    public class FileTail
    {
		readonly Stream _inputStream;

		public FileTail(Stream inputStream)
		{
			_inputStream = inputStream;
		}

		Protocol.PostScript ReadPostScript(out byte postScriptLength)
		{
			_inputStream.Seek(-1, SeekOrigin.End);
			postScriptLength = _inputStream.CheckedReadByte();

			_inputStream.Seek(-1 - postScriptLength, SeekOrigin.End);
			var stream = new StreamSegment(_inputStream, postScriptLength, true);

			var postScript = Serializer.Deserialize<Protocol.PostScript>(stream);

			if (postScript.Magic != "ORC")
				throw new InvalidDataException("Postscript didn't contain magic bytes");

			return postScript;
		}

		Protocol.Footer ReadFooter(Protocol.PostScript postScript, byte postScriptLength)
		{
			_inputStream.Seek(-1 - postScriptLength - (long)postScript.FooterLength, SeekOrigin.End);
			var compressedStream = new StreamSegment(_inputStream, (long)postScript.FooterLength, true);
			var footerStream = OrcCompressedStream.GetDecompressingStream(compressedStream, postScript.Compression);

			return Serializer.Deserialize<Protocol.Footer>(footerStream);
		}

		public StripeReaderCollection GetStripeCollection()
		{
			byte postScriptLength;
			var postScript = ReadPostScript(out postScriptLength);
			var footer = ReadFooter(postScript, postScriptLength);

			return new StripeReaderCollection(_inputStream, footer, postScript.Compression);
		}
    }
}
