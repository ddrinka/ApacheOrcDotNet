using System;
using System.Collections.Generic;

namespace ApacheOrcDotNet {
    public interface IOrcWriter : IDisposable {
        /// <summary>
        /// Add metadata to the ORC file. Calling again with an identical key overwrites the original value.
        /// </summary>
        /// <param name="key">Label for metadata</param>
        /// <param name="value">Contents of the metadata</param>
        void AddUserMetadata(string key, byte[] value);

        /// <summary>
        /// Add a single row
        /// </summary>
        void AddRow(object row);

        /// <summary>
        /// Add all rows in the provided enumerable
        /// </summary>
        void AddRows(IEnumerable<object> rows);
    }
}
