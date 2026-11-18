// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Microsoft.AspNet.SignalR.Infrastructure
{
    /// <summary>
    /// The layout of a binary WebSocket message: the JSON message followed by the values that were taken out of it,
    /// each block prefixed with its length as a little-endian Int32. Shared by the server and the client.
    /// </summary>
    internal static class BinaryEnvelope
    {
        public const string PlaceholderName = "$binary";

        /// <summary>
        /// The JSON written in place of a value. The n-th placeholder of the JSON message stands for the n-th attachment.
        /// </summary>
        public const string Placeholder = "{\"" + PlaceholderName + "\":true}";

        /// <summary>
        /// The error for a value that is serialized as the placeholder itself: whoever reads the message would take it for a binary value.
        /// </summary>
        public const string ReservedValueError = "A value that is serialized as " + Placeholder + " can't be sent, that JSON is reserved for binary values.";

        /// <summary>
        /// The number of placeholders in a JSON message.
        /// A JSON string can't contain the placeholder text (its quotes would be escaped), so every match is a value.
        /// </summary>
        public static int CountPlaceholders(ArraySegment<byte> json)
        {
            int count = 0;
            int last = json.Offset + json.Count - Placeholder.Length;

            for (int i = json.Offset; i <= last; i++)
            {
                int matched = 0;

                while (matched < Placeholder.Length && json.Array[i + matched] == Placeholder[matched])
                {
                    matched++;
                }

                if (matched == Placeholder.Length)
                {
                    count++;
                    i += Placeholder.Length - 1;
                }
            }

            return count;
        }

        public static ArraySegment<byte> Write(ArraySegment<byte> json, IList<byte[]> attachments)
        {
            int length = sizeof(int) + json.Count;

            for (int i = 0; i < attachments.Count; i++)
            {
                length += sizeof(int) + attachments[i].Length;
            }

            var buffer = new byte[length];

            using (var writer = new BinaryWriter(new MemoryStream(buffer)))
            {
                writer.Write(json.Count);
                writer.Write(json.Array, json.Offset, json.Count);

                for (int i = 0; i < attachments.Count; i++)
                {
                    writer.Write(attachments[i].Length);
                    writer.Write(attachments[i]);
                }
            }

            return new ArraySegment<byte>(buffer);
        }

        public static List<byte[]> Read(byte[] message, out string json)
        {
            using (var reader = new BinaryReader(new MemoryStream(message)))
            {
                byte[] block = ReadBlock(reader);
                json = Encoding.UTF8.GetString(block, 0, block.Length);

                var attachments = new List<byte[]>();

                while (reader.BaseStream.Position < reader.BaseStream.Length)
                {
                    attachments.Add(ReadBlock(reader));
                }

                return attachments;
            }
        }

        private static byte[] ReadBlock(BinaryReader reader)
        {
            long remaining = reader.BaseStream.Length - reader.BaseStream.Position - sizeof(int);
            int length = remaining < 0 ? -1 : reader.ReadInt32();

            if (length < 0 || length > remaining)
            {
                throw new InvalidDataException("The binary message is truncated.");
            }

            return reader.ReadBytes(length);
        }
    }
}
