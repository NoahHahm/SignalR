using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNet.SignalR;
using Microsoft.AspNet.SignalR.Infrastructure;
using Newtonsoft.Json.Linq;

namespace BinaryPayloadScenario
{
    /// <summary>
    /// Runs every scenario against a server in this process and prints what passed. The exit code is the number of failures.
    /// </summary>
    public static class Program
    {
        private const string HubData = "[{\"name\":\"scenariohub\"}]";

        private static int _number;
        private static int _failed;

        public static async Task<int> Main()
        {
            ScenarioHost host = await ScenarioHost.Start();
            Console.WriteLine("Server listening on " + host.Url);
            Console.WriteLine();

            ScenarioClient wsA = await ScenarioClient.WebSockets(host, "ws-a");
            ScenarioClient wsB = await ScenarioClient.WebSockets(host, "ws-b");
            ScenarioClient wsNoHook = await ScenarioClient.WebSockets(host, "ws-no-hook", hook: false);
            ScenarioClient sse = await ScenarioClient.ServerSentEvents(host, "sse");
            ScenarioClient polling = await ScenarioClient.LongPolling(host, "long-polling");
            ScenarioClient auto = await ScenarioClient.Auto(host, "auto");
            ScenarioClient wsScaleout = await ScenarioClient.WebSockets(host, "ws-scaleout", path: "/scaleout");
            ScenarioClient wsReconnect = await ScenarioClient.WebSockets(host, "ws-reconnect");
            ScenarioClient wsPlain = await ScenarioClient.WebSockets(host, "ws-plain", hook: false, path: "/plain");

            var everyone = new[] { wsA, wsB, wsNoHook, sse, polling, auto };
            var webSocketClients = new[] { wsA, wsB, wsNoHook, auto };

            Console.WriteLine("Client to server");

            await Run("An object argument is sent as a binary message and bound to a byte[] parameter", async () =>
            {
                PlayerState state = PlayerState.Create(1, "alice");
                Mark mark = Mark.Of(wsA);
                int taken = wsA.Hook.Taken;

                int length = await wsA.Invoke<int>("Length", state);

                Equal(state.Pack().Length, length, "the length the hub method saw");
                Equal(taken + 1, wsA.Hook.Taken, "the values the client hook took");
                mark.Expect(fromClient: 1, toClient: 0);
            });

            await Run("Binary and JSON arguments are mixed in one invocation, in order", async () =>
            {
                Mark mark = Mark.Of(wsA);
                int taken = wsA.Hook.Taken;

                string result = await wsA.Invoke<string>("Mixed", "text", PlayerState.Create(2, "first"), 7, PlayerState.Create(3, "second"));

                Equal("text|first|7|second", result, "what the hub method saw");
                Equal(taken + 2, wsA.Hook.Taken, "the values the client hook took");
                mark.Expect(fromClient: 1, toClient: 0);
            });

            await Run("An invocation without anything for the hook stays a text message", async () =>
            {
                Mark mark = Mark.Of(wsA);

                string result = await wsA.Invoke<string>("Plain", "hello");

                Equal("HELLO", result, "the result");
                mark.Expect(fromClient: 0, toClient: 0);
            });

            await Run("A 1MB payload goes both ways unchanged", async () =>
            {
                byte[] payload = RandomBytes(1024 * 1024);
                Mark mark = Mark.Of(wsA);

                byte[] result = await wsA.Invoke<byte[]>("Echo", payload);

                Same(payload, result, "the echoed payload");
                mark.Expect(fromClient: 1, toClient: 1);
            });

            await Run("AutoTransport passes the hook of the WebSocket transport it picked", async () =>
            {
                PlayerState state = PlayerState.Create(4, "auto");
                Mark mark = Mark.Of(auto);

                int length = await auto.Invoke<int>("Length", state);

                Equal("webSockets", auto.Connection.Transport.Name, "the transport that was picked");
                Equal(state.Pack().Length, length, "the length the hub method saw");
                mark.Expect(fromClient: 1, toClient: 0);
            });

            await Run("A client without a hook sends a byte[] as JSON and still receives binary", async () =>
            {
                byte[] payload = RandomBytes(1000);
                Mark mark = Mark.Of(wsNoHook);

                byte[] result = await wsNoHook.Invoke<byte[]>("Echo", payload);

                Same(payload, result, "the echoed payload");
                mark.Expect(fromClient: 0, toClient: 1);
            });

            Console.WriteLine();
            Console.WriteLine("Server to client");

            await Run("A returned object is sent as a binary message and received as a byte[]", async () =>
            {
                Mark mark = Mark.Of(wsA);
                int taken = host.ServerHook.Taken;

                byte[] result = await wsA.Invoke<byte[]>("Make", 5, "bob");

                Same(PlayerState.Create(5, "bob").Pack(), result, "the returned payload");
                Equal(taken + 1, host.ServerHook.Taken, "the values the server hook took");
                mark.Expect(fromClient: 0, toClient: 1);
            });

            await Run("A client method invoked from a hub gets its object argument as a byte[]", async () =>
            {
                PlayerState state = PlayerState.Create(6, "pushed");
                string tag = NewTag();
                Mark mark = Mark.Of(wsA);

                await wsA.Invoke("PushToCaller", tag, state);
                object[] args = await wsA.Push(tag);

                Same(state.Pack(), (byte[])args[0], "the pushed payload");
                // The push; the result of the invocation itself is a text message
                mark.Expect(fromClient: 1, toClient: 1);
            });

            await Run("Two binary arguments with a JSON one in between arrive in order", async () =>
            {
                PlayerState first = PlayerState.Create(7, "first");
                PlayerState second = PlayerState.Create(8, "second");
                string tag = NewTag();
                Mark mark = Mark.Of(wsA);

                await wsA.Invoke("PushPairToCaller", tag, first, second);
                object[] args = await wsA.Push(tag);

                Same(first.Pack(), (byte[])args[0], "the first payload");
                Equal("between", (string)args[1], "the JSON argument");
                Same(second.Pack(), (byte[])args[2], "the second payload");
                mark.Expect(fromClient: 1, toClient: 1);
            });

            await Run("A broadcast from outside a hub reaches every transport with the same bytes", async () =>
            {
                PlayerState state = PlayerState.Create(9, "outside");
                string tag = NewTag();
                Mark[] marks = webSocketClients.Select(Mark.Of).ToArray();

                IHubContext context = host.Resolver.Resolve<IConnectionManager>().GetHubContext<ScenarioHub>();
                Task send = context.Clients.All.onState(tag, state);
                await send;

                foreach (ScenarioClient client in everyone)
                {
                    object[] args = await client.Push(tag);
                    Same(state.Pack(), (byte[])args[0], "the payload " + client.Name + " received");
                }

                foreach (Mark mark in marks)
                {
                    mark.Expect(fromClient: 0, toClient: 1);
                }
            });

            await Run("A broadcast from a hub reaches every transport with the same bytes", async () =>
            {
                PlayerState state = PlayerState.Create(10, "everyone");
                string tag = NewTag();

                await wsB.Invoke("PushToAll", tag, state);

                foreach (ScenarioClient client in everyone)
                {
                    object[] args = await client.Push(tag);
                    Same(state.Pack(), (byte[])args[0], "the payload " + client.Name + " received");
                }
            });

            await Run("A send to several connections at once reaches each of them", async () =>
            {
                PlayerState state = PlayerState.Create(11, "several");
                string tag = NewTag();
                var targets = new[] { wsA, sse, polling };
                Mark mark = Mark.Of(wsA);

                await wsB.Invoke("PushToMany", tag, targets.Select(client => client.Connection.ConnectionId).ToArray(), state);

                foreach (ScenarioClient client in targets)
                {
                    object[] args = await client.Push(tag);
                    Same(state.Pack(), (byte[])args[0], "the payload " + client.Name + " received");
                }

                mark.Expect(fromClient: 0, toClient: 1);
            });

            await Run("200 messages published at once arrive complete and in order", async () =>
            {
                const int count = 200;
                string tag = NewTag();
                Mark mark = Mark.Of(wsA);

                await wsA.Invoke("Burst", tag, count);

                foreach (ScenarioClient client in new[] { wsA, sse, polling })
                {
                    List<KeyValuePair<int, byte[]>> items = await client.Burst(tag).Take(count);
                    Equal(count, items.Count, "the number of messages " + client.Name + " received");

                    for (int i = 0; i < count; i++)
                    {
                        Equal(i, items[i].Key, "the index of message " + i + " for " + client.Name);
                        Same(PlayerState.Create(i, "burst-" + i).Pack(), items[i].Value, "the payload of message " + i + " for " + client.Name);
                    }
                }

                int frames = wsA.Frames.BinaryToClient - mark.ToClient;
                True(frames >= 1 && frames <= count, "between 1 and " + count + " binary messages", frames + " binary messages");

                return count + " messages in " + frames + " binary WebSocket messages";
            });

            Console.WriteLine();
            Console.WriteLine("Other transports and scaleout");

            foreach (ScenarioClient client in new[] { sse, polling })
            {
                await Run(client.Name + ": arguments and results keep their JSON form, a byte[] is base64", async () =>
                {
                    PlayerState state = PlayerState.Create(12, client.Name);
                    byte[] payload = RandomBytes(1000);

                    PlayerState echoed = await client.Invoke<PlayerState>("Json", state);
                    PlayerState made = await client.Invoke<PlayerState>("Make", 13, "json");
                    byte[] result = await client.Invoke<byte[]>("Echo", payload);

                    Same(state.Pack(), echoed.Pack(), "the object sent and returned as JSON");
                    Same(PlayerState.Create(13, "json").Pack(), made.Pack(), "the object returned as JSON");
                    Same(payload, result, "the echoed payload");
                });
            }

            await Run("Behind a backplane a push is delivered as base64, a result is still binary", async () =>
            {
                PlayerState state = PlayerState.Create(14, "scaleout");
                string tag = NewTag();
                int taken = host.ScaleoutServerHook.Taken;
                Mark mark = Mark.Of(wsScaleout);

                await wsScaleout.Invoke("PushToCaller", tag, state);
                object[] args = await wsScaleout.Push(tag);

                Same(state.Pack(), (byte[])args[0], "the pushed payload");
                Equal(taken + 1, host.ScaleoutServerHook.Taken, "the values the server hook took");
                // The payload went through the backplane inside the JSON, so the push is a text message
                mark.Expect(fromClient: 1, toClient: 0);

                mark = Mark.Of(wsScaleout);
                byte[] result = await wsScaleout.Invoke<byte[]>("Make", 15, "direct");

                Same(PlayerState.Create(15, "direct").Pack(), result, "the returned payload");
                mark.Expect(fromClient: 0, toClient: 1);
            });

            Console.WriteLine();
            Console.WriteLine("Concurrency and delivery");

            await Run("Concurrent invocations on one connection each get their own payload back", async () =>
            {
                await Task.WhenAll(Enumerable.Range(0, 400).Select(i => Task.Run(async () =>
                {
                    byte[] payload = PlayerState.Create(i, "concurrent-" + i, blobSize: 64 + i).Pack();
                    byte[] result = await wsA.Invoke<byte[]>("Echo", payload);

                    Same(payload, result, "the payload of invocation " + i);
                })));
            });

            await Run("Messages published from many threads at once keep their own payload", async () =>
            {
                const int count = 300;
                string tag = NewTag();

                await wsA.Invoke("Storm", tag, count);

                foreach (ScenarioClient client in new[] { wsA, wsB, sse })
                {
                    List<KeyValuePair<int, byte[]>> items = await client.Burst(tag).Take(count);

                    Equal(count, items.Select(item => item.Key).Distinct().Count(), "the number of different messages " + client.Name + " received");

                    foreach (KeyValuePair<int, byte[]> item in items)
                    {
                        Same(PlayerState.Create(item.Key, "burst-" + item.Key).Pack(), item.Value, "the payload of message " + item.Key + " for " + client.Name);
                    }
                }
            });

            await Run("A response that leaves out messages for the caller keeps the other payloads in place", async () =>
            {
                const int count = 200;
                string tag = NewTag();

                await wsA.Invoke("BurstOthers", tag, count);

                List<KeyValuePair<int, byte[]>> others = await wsB.Burst(tag).Take(count);
                List<KeyValuePair<int, byte[]>> caller = await wsA.Burst(tag).Take(count / 2);

                Equal(count, others.Count, "the number of messages " + wsB.Name + " received");
                Equal(count / 2, caller.Count, "the number of messages the caller received");

                for (int i = 0; i < count; i++)
                {
                    Equal(i, others[i].Key, "the index of message " + i + " for " + wsB.Name);
                    Same(PlayerState.Create(i, "burst-" + i).Pack(), others[i].Value, "the payload of message " + i + " for " + wsB.Name);
                }

                for (int i = 0; i < count / 2; i++)
                {
                    Equal(i * 2, caller[i].Key, "the index of message " + i + " for the caller");
                    Same(PlayerState.Create(i * 2, "burst-" + i * 2).Pack(), caller[i].Value, "the payload of message " + i + " for the caller");
                }
            });

            await Run("Messages published while a client is reconnecting are delivered afterwards, as they were published", async () =>
            {
                var reconnecting = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                var reconnected = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
                wsReconnect.Connection.Reconnecting += () => reconnecting.TrySetResult(null);
                wsReconnect.Connection.Reconnected += () => reconnected.TrySetResult(null);

                dynamic target = host.Resolver.Resolve<IConnectionManager>().GetHubContext<ScenarioHub>().Clients.Client(wsReconnect.Connection.ConnectionId);
                string firstTag = NewTag();
                string secondTag = NewTag();
                PlayerState second = PlayerState.Create(21, "object");
                byte[] buffer = PlayerState.Create(20, "buffer").Pack();
                byte[] published = (byte[])buffer.Clone();
                Mark mark = Mark.Of(wsReconnect);

                host.Drop(wsReconnect.Connection.ConnectionToken);
                await reconnecting.Task.WaitAsync(ScenarioClient.Timeout);

                await (Task)target.onState(firstTag, buffer);
                // The application reuses its buffer once the send has completed
                Array.Clear(buffer, 0, buffer.Length);
                await (Task)target.onState(secondTag, second);

                await reconnected.Task.WaitAsync(ScenarioClient.Timeout);

                Same(published, (byte[])(await wsReconnect.Push(firstTag))[0], "the payload published from a buffer");
                Same(second.Pack(), (byte[])(await wsReconnect.Push(secondTag))[0], "the payload published from an object");

                int frames = wsReconnect.Frames.BinaryToClient - mark.ToClient;
                True(frames == 1 || frames == 2, "the two messages in 1 or 2 binary messages", frames + " binary messages");
            });

            Console.WriteLine();
            Console.WriteLine("Values");

            await Run("An empty payload, a null and an asynchronous result stay what they are", async () =>
            {
                byte[] empty = await wsA.Invoke<byte[]>("Echo", new byte[0]);
                byte[] none = await wsA.Invoke<byte[]>("Echo", new object[] { null });
                byte[] nothing = await wsA.Invoke<byte[]>("Nothing");
                Mark mark = Mark.Of(wsA);
                byte[] made = await wsA.Invoke<byte[]>("MakeAsync", 22, "async");

                True(empty != null && empty.Length == 0, "an empty array for an empty payload", empty == null ? "null" : empty.Length + " bytes");
                True(none == null, "null for a null argument", "a value");
                True(nothing == null, "null for a null result", "a value");
                Same(PlayerState.Create(22, "async").Pack(), made, "the payload returned from a task");
                mark.Expect(fromClient: 0, toClient: 1);
            });

            await Run("Text outside ASCII next to a payload arrives unchanged", async () =>
            {
                const string text = "한글 テスト \U0001F3AE \"quoted\" \\ \u2028 end";
                PlayerState first = PlayerState.Create(23, "이름");
                PlayerState second = PlayerState.Create(24, "名前");
                string tag = NewTag();

                string result = await wsA.Invoke<string>("Mixed", text, first, 7, second);
                await wsA.Invoke("PushTextToCaller", tag, text, first);
                object[] args = await wsA.Push(tag);

                Equal(text + "|이름|7|名前", result, "what the hub method saw");
                Same(first.Pack(), (byte[])args[0], "the payload before the text");
                Equal(text, (string)args[1], "the text");
                Same(first.Pack(), (byte[])args[2], "the payload after the text");
            });

            await Run("A binary argument for a parameter that is not a byte[] fails the invocation, not the connection", async () =>
            {
                string error = null;

                try
                {
                    // The hook turns the object into bytes, the hub method wants the object
                    await wsA.Invoke<JToken>("Json", PlayerState.Create(25, "wrong"));
                }
                catch (Exception ex) when (!(ex is TimeoutException))
                {
                    error = ex.Message;
                }

                True(error != null, "the invocation to fail", "a result");
                Equal("STILL UP", await wsA.Invoke<string>("Plain", "still up"), "the result of an invocation afterwards");

                return error;
            });

            await Run("Progress values are not passed to the hook, they keep their JSON form", async () =>
            {
                var reported = new List<JToken>();
                Mark mark = Mark.Of(wsA);

                int count = await wsA.Hub.Invoke<int, JToken>("Report", value => { lock (reported) { reported.Add(value); } }, 3).WaitAsync(ScenarioClient.Timeout);

                Equal(3, count, "the result");
                Equal(3, reported.Count, "the number of progress values");
                Equal("progress-0", (string)reported[0]["Name"], "the name in the first progress value");
                mark.Expect(fromClient: 0, toClient: 0);
            });

            await Run("An invocation on a stopped connection fails the same way with and without a binary argument", async () =>
            {
                ScenarioClient stopped = await ScenarioClient.WebSockets(host, "ws-stopped");
                stopped.Connection.Stop(TimeSpan.FromSeconds(2));
                Task<Exception> error = stopped.NextError();

                string text = await Failure(() => stopped.Hub.Invoke<string>("Plain", "text"));
                string binary = await Failure(() => stopped.Hub.Invoke<int>("Length", PlayerState.Create(28, "stopped")));

                Equal(text, binary, "the error of the binary invocation");
                True(!error.IsCompleted, "no Error event", "one");
            });

            Console.WriteLine();
            Console.WriteLine("The reserved value: {\"$binary\":true} stands for a binary value, so it can't be sent as JSON");

            const string reserved = "{\"$binary\":true}";

            await Run("Client to server: next to a binary argument it is refused before anything is sent", async () =>
            {
                PlayerState state = PlayerState.Create(26, "flags");
                Mark mark = Mark.Of(wsA);
                Task<Exception> error = wsA.NextError();

                string failure = await Failure(() => wsA.Hub.Invoke<string>("Flags", new Dictionary<string, bool> { { "$binary", true } }, state));

                True(failure.Contains("reserved"), "the invocation to fail because the value is reserved", failure);
                mark.Expect(fromClient: 0, toClient: 0);
                True(!error.IsCompleted, "no Error event", "one");
                Equal("other|flags", await wsA.Invoke<string>("Flags", new Dictionary<string, bool> { { "other", true } }, state), "the result of an invocation afterwards");

                return failure;
            });

            foreach (ScenarioClient client in new[] { wsA, sse })
            {
                await Run("Server to " + client.Name + ": it is refused when it is published, alone, next to a binary value and inside another value", async () =>
                {
                    byte[] payload = RandomBytes(64);
                    Task<Exception> error = client.NextError();
                    string failure = null;

                    foreach (string shape in new[] { reserved, "{\"inner\":" + reserved + "}", "[1," + reserved + "]" })
                    {
                        foreach (byte[] data in new[] { payload, null })
                        {
                            failure = await Failure(() => client.Invoke("PushShape", NewTag(), shape, data));

                            True(failure.Contains("reserved"), "publishing " + shape + (data == null ? " alone" : " next to a payload") + " to fail because the value is reserved", failure);
                        }
                    }

                    // Nothing was published, and the connection is as it was
                    string tag = NewTag();
                    await client.Invoke("PushShape", tag, "{\"fine\":true}", payload);
                    object[] pushed = await client.Push(tag);

                    Equal("{\"fine\":true}", ((JToken)pushed[0]).ToString(Newtonsoft.Json.Formatting.None), "the JSON argument of a push afterwards");
                    Same(payload, ((JToken)pushed[1]).ToObject<byte[]>(), "the payload of a push afterwards");
                    True(!error.IsCompleted, "no Error event", "one");

                    return failure;
                });

                await Run("Server to " + client.Name + ": a value that only looks similar is ordinary JSON", async () =>
                {
                    byte[] payload = RandomBytes(64);
                    Task<Exception> error = client.NextError();

                    foreach (string shape in new[] { "{\"$binary\":false}", "{\"$binary\":1}", "{\"$binary\":true,\"more\":1}", "{\"inner\":{\"$binary\":false}}", "\"{\\\"$binary\\\":true}\"" })
                    {
                        string tag = NewTag();
                        await client.Invoke("PushShape", tag, shape, payload);

                        Task<object[]> push = client.Push(tag);

                        if (await Task.WhenAny(push, error) == error)
                        {
                            throw new ScenarioException("The client dropped the response with " + shape + ": " + (await error).Message);
                        }

                        Equal(shape, ((JToken)(await push)[0]).ToString(Newtonsoft.Json.Formatting.None), "the JSON argument");
                        Same(payload, ((JToken)(await push)[1]).ToObject<byte[]>(), "the payload next to " + shape);
                    }
                });
            }

            await Run("Server to ws-a: a value that only looks similar in a message of its own doesn't disturb the binary messages around it", async () =>
            {
                string tag = NewTag();
                Task<Exception> error = wsA.NextError();

                await wsA.Invoke("BurstWithFlags", tag, 50);
                Task<List<KeyValuePair<int, byte[]>>> burst = wsA.Burst(tag).Take(50);

                if (await Task.WhenAny(burst, error) == error)
                {
                    throw new ScenarioException("The client dropped a whole response: " + (await error).Message);
                }

                foreach (KeyValuePair<int, byte[]> item in await burst)
                {
                    Same(PlayerState.Create(item.Key, "burst-" + item.Key).Pack(), item.Value, "payload " + item.Key);
                }
            });

            await Run("Server to ws-a: a hub state with the value next to a binary result is left as it is", async () =>
            {
                Task<Exception> error = wsA.NextError();
                Task<byte[]> made = wsA.Invoke<byte[]>("MakeWithState", 27, "state");

                if (await Task.WhenAny(made, error) == error)
                {
                    throw new ScenarioException("The client dropped the response: " + (await error).Message);
                }

                Same(PlayerState.Create(27, "state").Pack(), await made, "the result");
                Equal(reserved, wsA.Hub["flags"].ToString(Newtonsoft.Json.Formatting.None), "the hub state");
            });

            await Run("A server without the hook sends and receives the value as the JSON it is", async () =>
            {
                PlayerState state = PlayerState.Create(28, "plain");
                string tag = NewTag();

                string result = await wsPlain.Invoke<string>("Flags", new Dictionary<string, bool> { { "$binary", true } }, state.Pack());
                await wsPlain.Invoke("PushShape", tag, reserved, state.Pack());
                object[] pushed = await wsPlain.Push(tag);

                Equal("$binary|plain", result, "what the hub method saw");
                Equal(reserved, ((JToken)pushed[0]).ToString(Newtonsoft.Json.Formatting.None), "the JSON argument");
                Same(state.Pack(), ((JToken)pushed[1]).ToObject<byte[]>(), "the payload");
            });

            Console.WriteLine();
            Console.WriteLine("Wire format");

            await Run("A binary message is the JSON and then each payload, each prefixed with its Int32 length", async () =>
            {
                byte[] payload = RandomBytes(300);

                using (ClientWebSocket socket = await RawSocket(host))
                {
                    byte[] json = Encoding.UTF8.GetBytes("{\"H\":\"scenariohub\",\"M\":\"Echo\",\"A\":[{\"$binary\":true}],\"I\":0}");
                    await Send(socket, Envelope(json, payload));

                    byte[] response = await NextBinary(socket);

                    using (var reader = new BinaryReader(new MemoryStream(response)))
                    {
                        JObject result = JObject.Parse(Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadInt32())));
                        byte[] attachment = reader.ReadBytes(reader.ReadInt32());

                        Equal("{\"$binary\":true}", result["R"].ToString(Newtonsoft.Json.Formatting.None), "the placeholder of the result");
                        Equal("0", (string)result["I"], "the id of the invocation");
                        Same(payload, attachment, "the payload after the JSON");
                        Equal(response.Length, (int)reader.BaseStream.Position, "the end of the message");
                    }
                }
            });

            await Run("A truncated binary message closes that connection, the server keeps running", async () =>
            {
                using (ClientWebSocket socket = await RawSocket(host))
                {
                    // Announces 16 bytes of JSON and has one
                    await Send(socket, new byte[] { 16, 0, 0, 0, (byte)'{' });

                    True(await Closes(socket), "the connection to be closed", "a connection that stayed open");
                }

                Equal("STILL UP", await wsA.Invoke<string>("Plain", "still up"), "the result of an invocation afterwards");
            });

            await Run("A binary message whose payloads don't match its placeholders is ignored, the next one is served", async () =>
            {
                byte[] payload = RandomBytes(40);

                using (ClientWebSocket socket = await RawSocket(host))
                {
                    // Two placeholders with one payload, one placeholder with two payloads, and one that is right
                    await Send(socket, Envelope(Encoding.UTF8.GetBytes("{\"H\":\"scenariohub\",\"M\":\"Shapes\",\"A\":[{\"$binary\":true},{\"$binary\":true}],\"I\":0}"), payload));
                    await Send(socket, Envelope(Encoding.UTF8.GetBytes("{\"H\":\"scenariohub\",\"M\":\"Length\",\"A\":[{\"$binary\":true}],\"I\":1}"), payload, payload));
                    await Send(socket, Envelope(Encoding.UTF8.GetBytes("{\"H\":\"scenariohub\",\"M\":\"Length\",\"A\":[{\"$binary\":true}],\"I\":2}"), payload));

                    JObject result = await NextResult(socket);

                    Equal("2", (string)result["I"], "the first invocation that is answered (" + result.ToString(Newtonsoft.Json.Formatting.None) + ")");
                    Equal(40, (int)result["R"], "its result");
                }
            });

            await Run("A client that only ever sent text gets a binary message as well, binary is not negotiated", async () =>
            {
                using (ClientWebSocket socket = await RawSocket(host))
                {
                    using (var timeout = new CancellationTokenSource(ScenarioClient.Timeout))
                    {
                        byte[] invocation = Encoding.UTF8.GetBytes("{\"H\":\"scenariohub\",\"M\":\"Make\",\"A\":[5,\"raw\"],\"I\":0}");
                        await socket.SendAsync(new ArraySegment<byte>(invocation), WebSocketMessageType.Text, endOfMessage: true, cancellationToken: timeout.Token);
                    }

                    byte[] response = await NextBinary(socket);

                    True(response.Length > 0, "a binary message with the result", "nothing");
                }
            });

            foreach (ScenarioClient client in everyone.Concat(new[] { wsScaleout, wsReconnect, wsPlain }))
            {
                client.Dispose();
            }

            await host.DisposeAsync();

            Console.WriteLine();
            Console.WriteLine(_failed == 0
                ? _number + " scenarios passed."
                : _failed + " of " + _number + " scenarios FAILED.");

            return _failed;
        }

        private static Task Run(string name, Func<Task> scenario)
        {
            return Run(name, async () =>
            {
                await scenario();
                return null;
            });
        }

        private static async Task Run(string name, Func<Task<string>> scenario)
        {
            string label = (++_number).ToString("00") + "  " + name;

            try
            {
                string detail = await scenario();

                Console.WriteLine("  PASS  " + label + (detail == null ? "" : " (" + detail + ")"));
            }
            catch (Exception ex)
            {
                _failed++;

                Console.WriteLine("  FAIL  " + label);
                Console.WriteLine("        " + (ex is ScenarioException ? ex.Message : ex.ToString()).Replace("\n", "\n        "));
            }
        }

        // How an invocation failed, whether it threw or returned a failed task
        private static async Task<string> Failure(Func<Task> invoke)
        {
            try
            {
                await invoke().WaitAsync(ScenarioClient.Timeout);
                return "no error";
            }
            catch (Exception ex)
            {
                return ex.GetType().Name + ": " + ex.Message;
            }
        }

        private static string NewTag()
        {
            return Guid.NewGuid().ToString("N");
        }

        private static byte[] RandomBytes(int length)
        {
            var bytes = new byte[length];
            new Random(length).NextBytes(bytes);
            return bytes;
        }

        private static void Equal<T>(T expected, T actual, string what)
        {
            True(EqualityComparer<T>.Default.Equals(expected, actual), what + " to be " + expected, String.Concat(actual));
        }

        private static void Same(byte[] expected, byte[] actual, string what)
        {
            True(actual != null && expected.AsSpan().SequenceEqual(actual),
                 what + " to be the " + expected.Length + " bytes that were sent",
                 actual == null ? "null" : actual.Length + " bytes that differ");
        }

        private static void True(bool condition, string expected, string actual)
        {
            if (!condition)
            {
                throw new ScenarioException("Expected " + expected + ", got " + actual + ".");
            }
        }

        // What follows talks to the server without the SignalR client, to see the messages as they are on the wire

        private static async Task<ClientWebSocket> RawSocket(ScenarioHost host)
        {
            string connectionData = Uri.EscapeDataString(HubData);
            string token;

            using (var http = new HttpClient())
            {
                string negotiation = await http.GetStringAsync(host.Url + "/signalr/negotiate?clientProtocol=2.1&connectionData=" + connectionData);
                token = (string)JObject.Parse(negotiation)["ConnectionToken"];
            }

            var socket = new ClientWebSocket();
            string url = host.Url.Replace("http://", "ws://") + "/signalr/connect?transport=webSockets&clientProtocol=2.1&connectionToken=" +
                         Uri.EscapeDataString(token) + "&connectionData=" + connectionData;

            using (var timeout = new CancellationTokenSource(ScenarioClient.Timeout))
            {
                await socket.ConnectAsync(new Uri(url), timeout.Token);
            }

            return socket;
        }

        private static byte[] Envelope(byte[] json, params byte[][] attachments)
        {
            var stream = new MemoryStream();

            using (var writer = new BinaryWriter(stream))
            {
                writer.Write(json.Length);
                writer.Write(json);

                foreach (byte[] attachment in attachments)
                {
                    writer.Write(attachment.Length);
                    writer.Write(attachment);
                }
            }

            return stream.ToArray();
        }

        private static async Task Send(ClientWebSocket socket, byte[] message)
        {
            using (var timeout = new CancellationTokenSource(ScenarioClient.Timeout))
            {
                await socket.SendAsync(new ArraySegment<byte>(message), WebSocketMessageType.Binary, endOfMessage: true, cancellationToken: timeout.Token);
            }
        }

        // The next binary message, skipping the text messages (the init message and keep alives)
        private static async Task<byte[]> NextBinary(ClientWebSocket socket)
        {
            using (var timeout = new CancellationTokenSource(ScenarioClient.Timeout))
            {
                var buffer = new byte[8192];

                while (true)
                {
                    var message = new MemoryStream();
                    WebSocketReceiveResult result;

                    do
                    {
                        result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), timeout.Token);
                        message.Write(buffer, 0, result.Count);
                    }
                    while (!result.EndOfMessage);

                    if (result.MessageType == WebSocketMessageType.Binary)
                    {
                        return message.ToArray();
                    }

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        throw new ScenarioException("Expected a binary message, got a closed connection.");
                    }
                }
            }
        }

        // The next result of a hub invocation, skipping everything else (the init message, keep alives)
        private static async Task<JObject> NextResult(ClientWebSocket socket)
        {
            using (var timeout = new CancellationTokenSource(ScenarioClient.Timeout))
            {
                var buffer = new byte[8192];

                while (true)
                {
                    var message = new MemoryStream();
                    WebSocketReceiveResult result;

                    do
                    {
                        result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), timeout.Token);
                        message.Write(buffer, 0, result.Count);
                    }
                    while (!result.EndOfMessage);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        throw new ScenarioException("Expected the result of an invocation, got a closed connection.");
                    }

                    if (result.MessageType == WebSocketMessageType.Text)
                    {
                        JObject json = JObject.Parse(Encoding.UTF8.GetString(message.ToArray()));

                        if (json["I"] != null)
                        {
                            return json;
                        }
                    }
                }
            }
        }

        private static async Task<bool> Closes(ClientWebSocket socket)
        {
            using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            {
                var buffer = new byte[8192];

                try
                {
                    while (true)
                    {
                        WebSocketReceiveResult result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), timeout.Token);

                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            return true;
                        }
                    }
                }
                catch (WebSocketException)
                {
                    // Closed without a close message
                    return true;
                }
                catch (OperationCanceledException)
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// The binary WebSocket messages of a client at one moment, to count the ones a scenario adds.
        /// </summary>
        private sealed class Mark
        {
            private ScenarioClient _client;

            public int FromClient { get; private set; }

            public int ToClient { get; private set; }

            public static Mark Of(ScenarioClient client)
            {
                return new Mark { _client = client, FromClient = client.Frames.BinaryFromClient, ToClient = client.Frames.BinaryToClient };
            }

            public void Expect(int fromClient, int toClient)
            {
                Equal(fromClient, _client.Frames.BinaryFromClient - FromClient, "the binary messages " + _client.Name + " sent");
                Equal(toClient, _client.Frames.BinaryToClient - ToClient, "the binary messages sent to " + _client.Name);
            }
        }

        private sealed class ScenarioException : Exception
        {
            public ScenarioException(string message)
                : base(message)
            {
            }
        }
    }
}
