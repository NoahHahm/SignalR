// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

namespace Microsoft.AspNet.SignalR.Json
{
    /// <summary>
    /// Decides which hub method arguments and return values are sent to WebSocket clients as raw bytes instead of JSON.
    /// Register an implementation with the dependency resolver to turn this on; without one everything is sent as JSON.
    /// The implementation is called from many threads at the same time.
    /// </summary>
    public interface IBinaryPayloadSerializer
    {
        /// <summary>
        /// Called for every argument of a client hub method invocation and for every hub method return value.
        /// </summary>
        /// <param name="value">The value that is about to be sent.</param>
        /// <param name="payload">The bytes to send in place of the value.</param>
        /// <returns>true to send <paramref name="payload"/> as raw bytes, false to send the value as JSON.</returns>
        /// <remarks>
        /// The receiver gets the bytes as a byte[]. A client that is not connected over WebSockets receives them as a base64 string,
        /// the JSON form of a byte[]. Return values are only passed to this method for WebSocket clients.
        /// </remarks>
        bool TrySerialize(object value, out byte[] payload);
    }
}
