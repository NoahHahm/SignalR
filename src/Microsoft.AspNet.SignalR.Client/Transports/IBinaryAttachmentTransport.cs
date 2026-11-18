// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.Threading.Tasks;

namespace Microsoft.AspNet.SignalR.Client.Transports
{
    /// <summary>
    /// A transport that can send values as raw attachments next to the JSON.
    /// </summary>
    internal interface IBinaryAttachmentTransport
    {
        /// <summary>
        /// Decides which values are sent as attachments. Null when everything is sent as JSON.
        /// </summary>
        IBinaryPayloadSerializer BinaryPayloadSerializer { get; }

        /// <summary>
        /// Sends JSON data in which every <see cref="Microsoft.AspNet.SignalR.Infrastructure.BinaryEnvelope.Placeholder"/> stands for the next attachment.
        /// </summary>
        Task Send(IConnection connection, string data, IList<byte[]> attachments);
    }
}
