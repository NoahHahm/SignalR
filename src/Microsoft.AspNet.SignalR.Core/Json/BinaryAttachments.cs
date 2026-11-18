// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Microsoft.AspNet.SignalR.Hubs;
using Microsoft.AspNet.SignalR.Infrastructure;
using Newtonsoft.Json.Linq;

namespace Microsoft.AspNet.SignalR.Json
{
    /// <summary>
    /// A <see cref="TextWriter"/> for transports that can send byte[] values as raw attachments next to the JSON (WebSockets).
    /// The JSON written to it has a <see cref="BinaryEnvelope.Placeholder"/> in place of every value in <see cref="Attachments"/>.
    /// </summary>
    internal class BinaryAttachmentTextWriter : BinaryMemoryPoolTextWriter
    {
        public BinaryAttachmentTextWriter(IMemoryPool memory)
            : base(memory)
        {
            Attachments = new List<byte[]>();
        }

        public List<byte[]> Attachments { get; private set; }
    }

    /// <summary>
    /// A hub method argument that arrived as a binary attachment.
    /// </summary>
    internal class BinaryAttachmentValue : IJsonValue
    {
        private readonly byte[] _value;

        public BinaryAttachmentValue(byte[] value)
        {
            _value = value;
        }

        public object ConvertTo(Type type)
        {
            if (!CanConvertTo(type))
            {
                throw new InvalidOperationException("A binary argument can only be bound to a byte[] parameter.");
            }

            return _value;
        }

        public bool CanConvertTo(Type type)
        {
            return type.IsAssignableFrom(typeof(byte[]));
        }
    }

    internal static class BinaryAttachments
    {
        /// <summary>
        /// Takes the values the serializer wants to send as raw bytes out of a hub response or a client hub invocation.
        /// Returns what to serialize to JSON instead, with a placeholder in place of every value added to the attachments.
        /// </summary>
        public static object Extract(object value, IBinaryPayloadSerializer serializer, List<byte[]> attachments)
        {
            var response = value as HubResponse;

            if (response != null)
            {
                if (TryExtract(response.Result, serializer, attachments))
                {
                    response.Result = new JRaw(BinaryEnvelope.Placeholder);
                }

                return value;
            }

            var invocation = value as ClientHubInvocation;

            if (invocation == null || invocation.Args == null)
            {
                return value;
            }

            object[] args = null;

            for (int i = 0; i < invocation.Args.Length; i++)
            {
                if (TryExtract(invocation.Args[i], serializer, attachments))
                {
                    // The array belongs to the caller
                    args = args ?? (object[])invocation.Args.Clone();
                    args[i] = new JRaw(BinaryEnvelope.Placeholder);
                }
            }

            if (args == null)
            {
                return value;
            }

            return new ClientHubInvocation { Hub = invocation.Hub, Method = invocation.Method, Args = args, State = invocation.State };
        }

        /// <summary>
        /// Does the same for a value that is about to be stored in the message bus, replacing it with what to serialize instead.
        /// Returns the attachments to store with the message, null when there are none.
        /// </summary>
        public static IList<byte[]> Extract(ref object value, IBinaryPayloadSerializer serializer)
        {
            if (serializer == null)
            {
                return null;
            }

            var payloads = new List<byte[]>();
            value = Extract(value, serializer, payloads);

            // The message outlives this call, so it can't keep referencing arrays the caller may reuse
            return payloads.Count == 0 ? null : payloads.ConvertAll(payload => (byte[])payload.Clone());
        }

        /// <summary>
        /// Makes sure the only placeholders in the JSON of a message are the ones that stand for its attachments,
        /// before the message is stored: a value that is serialized as the placeholder itself can't be told apart from them.
        /// </summary>
        public static void Verify(ArraySegment<byte> json, IList<byte[]> attachments, IBinaryPayloadSerializer serializer)
        {
            // Without a serializer no message has attachments, so nothing is read as a placeholder
            if (serializer != null && BinaryEnvelope.CountPlaceholders(json) != (attachments == null ? 0 : attachments.Count))
            {
                throw new InvalidOperationException(BinaryEnvelope.ReservedValueError);
            }
        }

        private static bool TryExtract(object value, IBinaryPayloadSerializer serializer, List<byte[]> attachments)
        {
            byte[] payload;

            if (value == null || !serializer.TrySerialize(value, out payload) || payload == null)
            {
                return false;
            }

            attachments.Add(payload);
            return true;
        }

        /// <summary>
        /// Replaces the placeholder arguments of a hub request with the attachments that came with it.
        /// </summary>
        public static IJsonValue[] Restore(IJsonValue[] values, IList<byte[]> attachments)
        {
            var restored = new IJsonValue[values.Length];
            int placeholders = 0;

            for (int i = 0; i < values.Length; i++)
            {
                var raw = values[i] as JRawValue;

                if (raw != null && raw.IsBinaryPlaceholder)
                {
                    // Counted even when no attachment is left for it, so that the mismatch is noticed below
                    restored[i] = placeholders < attachments.Count ? new BinaryAttachmentValue(attachments[placeholders]) : null;
                    placeholders++;
                }
                else
                {
                    restored[i] = values[i];
                }
            }

            if (placeholders != attachments.Count)
            {
                throw new InvalidDataException("The binary attachments do not match the arguments of the hub request.");
            }

            return restored;
        }

        /// <summary>
        /// Writes the attachments back into the JSON as base64 strings, for transports that can't send them separately.
        /// </summary>
        public static string Inline(string json, IList<byte[]> attachments)
        {
            // A JSON string can't contain the placeholder text (its quotes would be escaped), so every match is a placeholder.
            var builder = new StringBuilder(json.Length);
            int offset = 0;

            for (int i = 0; i < attachments.Count; i++)
            {
                int index = json.IndexOf(BinaryEnvelope.Placeholder, offset, StringComparison.Ordinal);

                if (index < 0)
                {
                    throw new InvalidDataException("The binary attachments do not match the message.");
                }

                builder.Append(json, offset, index - offset);
                builder.Append('"').Append(Convert.ToBase64String(attachments[i])).Append('"');
                offset = index + BinaryEnvelope.Placeholder.Length;
            }

            builder.Append(json, offset, json.Length - offset);

            return builder.ToString();
        }
    }
}
