// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using Microsoft.AspNet.SignalR.Json;

namespace Microsoft.AspNet.SignalR.Messaging
{
    public class Message
    {
        private static readonly byte[] _zeroByteBuffer = new byte[0];
        private static readonly UTF8Encoding _encoding = new UTF8Encoding();

        public Message()
        {
            Encoding = _encoding;
        }

        public Message(string source, string key, string value)
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }

            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            Source = source;
            Key = key;
            Encoding = _encoding;
            Value = value == null ? new ArraySegment<byte>(_zeroByteBuffer) : new ArraySegment<byte>(Encoding.GetBytes(value));
        }

        public Message(string source, string key, ArraySegment<byte> value)
            : this()
        {
            if (source == null)
            {
                throw new ArgumentNullException("source");
            }

            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            Source = source;
            Key = key;
            Value = value;
        }

        /// <summary>
        /// Which connection the message originated from
        /// </summary>
        public string Source { get; set; }

        /// <summary>
        /// The signal for the message (connection id, group, etc)
        /// </summary>
        public string Key { get; set; }

        /// <summary>
        /// The message payload
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly", Justification = "This type is serialzied")]
        public ArraySegment<byte> Value { get; set; }

        /// <summary>
        /// The command id if this message is a command
        /// </summary>
        public string CommandId { get; set; }

        /// <summary>
        /// Determines if the caller should wait for acknowledgement for this message
        /// </summary>
        public bool WaitForAck { get; set; }

        /// <summary>
        /// Determines if this message is itself an ACK
        /// </summary>
        public bool IsAck { get; set; }

        /// <summary>
        /// A list of connection ids to filter out
        /// </summary>
        public string Filter { get; set; }

        /// <summary>
        /// The encoding of the message
        /// </summary>
        public Encoding Encoding { get; private set; }

        /// <summary>
        /// The payload id. Only used in scaleout scenarios
        /// </summary>
        public ulong MappingId { get; set; }

        /// <summary>
        /// The stream index this message came from. Only used the scaleout scenarios.
        /// </summary>
        public int StreamIndex { get; set; }

        /// <summary>
        /// The byte[] values that were taken out of <see cref="Value"/>, which has a placeholder in place of each one.
        /// </summary>
        internal IList<byte[]> Attachments { get; set; }

        public bool IsCommand
        {
            get
            {
                return !String.IsNullOrEmpty(CommandId);
            }
        }

        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "This may be expensive")]
        public string GetString()
        {
            // If there's no encoding this is a raw binary payload
            if (Encoding == null)
            {
                throw new NotSupportedException();
            }

            string value = Encoding.GetString(Value.Array, Value.Offset, Value.Count);

            return Attachments == null ? value : BinaryAttachments.Inline(value, Attachments);
        }

        public void WriteTo(Stream stream)
        {
            var binaryWriter = new BinaryWriter(stream);
            binaryWriter.Write(Source);
            binaryWriter.Write(Key);
            // Attachments are not part of the stream format, so they are written back into the value
            ArraySegment<byte> value = Attachments == null ? Value : new ArraySegment<byte>(Encoding.GetBytes(GetString()));
            binaryWriter.Write(value.Count);
            binaryWriter.Write(value.Array, value.Offset, value.Count);
            binaryWriter.Write(CommandId ?? String.Empty);
            binaryWriter.Write(WaitForAck);
            binaryWriter.Write(IsAck);
            binaryWriter.Write(Filter ?? String.Empty);
        }

        public static Message ReadFrom(Stream stream)
        {
            var message = new Message();
            var binaryReader = new BinaryReader(stream);
            message.Source = binaryReader.ReadString();
            message.Key = binaryReader.ReadString();
            int bytes = binaryReader.ReadInt32();
            message.Value = new ArraySegment<byte>(binaryReader.ReadBytes(bytes));
            message.CommandId = binaryReader.ReadString();
            message.WaitForAck = binaryReader.ReadBoolean();
            message.IsAck = binaryReader.ReadBoolean();
            message.Filter = binaryReader.ReadString();

            return message;
        }
    }
}
