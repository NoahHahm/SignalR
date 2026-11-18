// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the Apache License, Version 2.0. See License.txt in the project root for license information.

using System.Collections.Generic;
using System.IO;
using Microsoft.AspNet.SignalR.Infrastructure;
using Newtonsoft.Json.Linq;

namespace Microsoft.AspNet.SignalR.Client.Transports
{
    internal static class BinaryAttachments
    {
        private static readonly JToken Placeholder = JToken.Parse(BinaryEnvelope.Placeholder);

        /// <summary>
        /// Replaces every <see cref="BinaryEnvelope.Placeholder"/> of a response with the attachment that came with it.
        /// The server only writes one in place of the result of a hub invocation or an argument of a client method,
        /// so nothing else in the response is looked at.
        /// </summary>
        public static void Restore(JObject response, IList<byte[]> attachments)
        {
            // In the order the server collected the attachments in
            var placeholders = new List<JToken>();

            Add(placeholders, response["R"]);

            var messages = response["M"] as JArray;

            if (messages != null)
            {
                foreach (JToken message in messages)
                {
                    var arguments = message.Type == JTokenType.Object ? message["A"] as JArray : null;

                    if (arguments != null)
                    {
                        foreach (JToken argument in arguments)
                        {
                            Add(placeholders, argument);
                        }
                    }
                }
            }

            if (placeholders.Count != attachments.Count)
            {
                throw new InvalidDataException("The binary attachments do not match the message.");
            }

            for (int i = 0; i < placeholders.Count; i++)
            {
                placeholders[i].Replace(new JValue(attachments[i]));
            }
        }

        private static void Add(List<JToken> placeholders, JToken token)
        {
            if (token != null && JToken.DeepEquals(token, Placeholder))
            {
                placeholders.Add(token);
            }
        }
    }
}
