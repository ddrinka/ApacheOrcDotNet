using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Infrastructure;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.Infrastructure
{
    public class StreamSegment_Test
    {
		[Fact]
		public void Read_ZeroLength_ShouldReturn0()
		{
			var stream = new MemoryStream();    //Empty
			var segment = new StreamSegment(stream, 0, false);
			var result = ReadBytes(segment, 100);
			Assert.Equal(0, result);
		}

		[Fact]
		public void Read_Overrun_ShouldReturn0()
		{
			var stream = new MemoryStream(new byte[] { 0x01, 0x02, 0x03 });
			var segment = new StreamSegment(stream, 2, false);
			var successfulRead = ReadBytes(segment, 2);
			Assert.Equal(2, successfulRead);
			var unsuccessfulRead = ReadBytes(segment, 1);
			Assert.Equal(0, unsuccessfulRead);
		}

		[Fact]
		public void Read_OneByteAtATime_ShouldReturnCorrectResult()
		{
			var stream = new MemoryStream(new byte[] { 0x01, 0x02, 0x03 });
			var segment = new StreamSegment(stream, 3, false);
			Assert.Equal(0x01, segment.ReadByte());
			Assert.Equal(0x02, segment.ReadByte());
			Assert.Equal(0x03, segment.ReadByte());
		}

		int ReadBytes(Stream stream, int numBytes)
		{
			var tempBuffer = new byte[numBytes];
			return stream.Read(tempBuffer, 0, numBytes);
		}
    }
}
