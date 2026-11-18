// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

namespace Microsoft.AspNet.SignalR.Client
{
    /// <summary>
    /// Decides which hub method arguments are sent over WebSockets as raw bytes instead of JSON.
    /// Set an implementation on the WebSocket transport to turn this on; without one everything is sent as JSON.
    /// The receiver gets the bytes as a byte[], the same as when they are sent as JSON (a base64 string).
    /// The implementation is called from many threads at the same time.
    /// </summary>
    public interface IBinaryPayloadSerializer
    {
        /// <summary>
        /// Called for every argument of a hub method invocation.
        /// </summary>
        /// <param name="value">The value that is about to be sent.</param>
        /// <param name="payload">The bytes to send in place of the value.</param>
        /// <returns>true to send <paramref name="payload"/> as raw bytes, false to send the value as JSON.</returns>
        bool TrySerialize(object value, out byte[] payload);
    }
}
