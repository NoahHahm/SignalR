using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNet.SignalR.Client;
using Microsoft.AspNet.SignalR.Client.Http;
using Microsoft.AspNet.SignalR.Client.Transports;
using Newtonsoft.Json.Linq;

namespace BinaryPayloadScenario
{
    /// <summary>
    /// One connected client and what the server pushed to it.
    /// </summary>
    public sealed class ScenarioClient : IDisposable
    {
        public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(15);

        private readonly ConcurrentDictionary<string, TaskCompletionSource<object[]>> _pushes = new ConcurrentDictionary<string, TaskCompletionSource<object[]>>();
        private readonly ConcurrentDictionary<string, BurstInbox> _bursts = new ConcurrentDictionary<string, BurstInbox>();

        private volatile TaskCompletionSource<Exception> _error;

        private ScenarioClient()
        {
        }

        public string Name { get; private set; }

        public HubConnection Connection { get; private set; }

        public IHubProxy Hub { get; private set; }

        /// <summary>
        /// The hook of this client, null when it has none.
        /// </summary>
        public MessagePackPayloadSerializer Hook { get; private set; }

        /// <summary>
        /// The WebSocket messages the server exchanged with this client.
        /// </summary>
        public FrameCounts Frames { get; private set; }

        public static Task<ScenarioClient> WebSockets(ScenarioHost host, string name, bool hook = true, string path = "/signalr")
        {
            var serializer = hook ? new MessagePackPayloadSerializer() : null;

            // This is all an application does to turn the feature on
            var transport = new WebSocketTransport { BinaryPayloadSerializer = serializer };

            return Connect(host, name, path, transport, serializer);
        }

        public static Task<ScenarioClient> ServerSentEvents(ScenarioHost host, string name)
        {
            return Connect(host, name, "/signalr", new ServerSentEventsTransport(), hook: null);
        }

        public static Task<ScenarioClient> LongPolling(ScenarioHost host, string name)
        {
            return Connect(host, name, "/signalr", new LongPollingTransport(), hook: null);
        }

        /// <summary>
        /// The WebSocket transport with its fallbacks, the way AutoTransport sets itself up.
        /// </summary>
        public static Task<ScenarioClient> Auto(ScenarioHost host, string name)
        {
            var serializer = new MessagePackPayloadSerializer();
            var httpClient = new DefaultHttpClient();

            var transport = new AutoTransport(httpClient, new IClientTransport[]
            {
                new WebSocketTransport(httpClient) { BinaryPayloadSerializer = serializer },
                new ServerSentEventsTransport(httpClient),
                new LongPollingTransport(httpClient)
            });

            return Connect(host, name, "/signalr", transport, serializer);
        }

        private static async Task<ScenarioClient> Connect(ScenarioHost host, string name, string path, IClientTransport transport, MessagePackPayloadSerializer hook)
        {
            var client = new ScenarioClient { Name = name, Hook = hook };

            client.Connection = new HubConnection(host.Url + path, useDefaultUrl: false);
            client.Hub = client.Connection.CreateHubProxy("ScenarioHub");

            // Whatever the transport, a binary payload arrives as a byte[]
            client.Hub.On<string, byte[]>("onState", (tag, data) => client.Pushed(tag, data));
            client.Hub.On<string, byte[], string, byte[]>("onPair", (tag, first, text, second) => client.Pushed(tag, first, text, second));
            client.Hub.On<string, int, byte[]>("onBurst", (tag, index, data) => client.Burst(tag).Add(index, data));
            // Loosely typed, to see what really arrives
            client.Hub.On<string, JToken, JToken>("onFlags", (tag, flags, data) => client.Pushed(tag, flags, data));
            client.Connection.Error += client.Failed;

            await client.Connection.Start(transport).WaitAsync(Timeout);

            client.Frames = host.FramesFor(client.Connection.ConnectionToken);

            return client;
        }

        public Task<T> Invoke<T>(string method, params object[] args)
        {
            return Hub.Invoke<T>(method, args).WaitAsync(Timeout);
        }

        public Task Invoke(string method, params object[] args)
        {
            return Hub.Invoke(method, args).WaitAsync(Timeout);
        }

        /// <summary>
        /// The arguments of the push with this tag, without the tag.
        /// </summary>
        public Task<object[]> Push(string tag)
        {
            return _pushes.GetOrAdd(tag, _ => NewSource()).Task.WaitAsync(Timeout);
        }

        /// <summary>
        /// The next error the connection reports.
        /// </summary>
        public Task<Exception> NextError()
        {
            var source = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
            _error = source;
            return source.Task;
        }

        private void Failed(Exception error)
        {
            TaskCompletionSource<Exception> source = _error;

            if (source != null)
            {
                source.TrySetResult(error);
            }
        }

        public BurstInbox Burst(string tag)
        {
            return _bursts.GetOrAdd(tag, _ => new BurstInbox());
        }

        private void Pushed(string tag, params object[] args)
        {
            _pushes.GetOrAdd(tag, _ => NewSource()).TrySetResult(args);
        }

        private static TaskCompletionSource<object[]> NewSource()
        {
            return new TaskCompletionSource<object[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public void Dispose()
        {
            Connection.Stop(TimeSpan.FromSeconds(2));
        }

        public sealed class BurstInbox
        {
            private readonly List<KeyValuePair<int, byte[]>> _items = new List<KeyValuePair<int, byte[]>>();
            private readonly TaskCompletionSource<object> _complete = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            private int _expected = -1;

            public void Add(int index, byte[] data)
            {
                lock (_items)
                {
                    _items.Add(new KeyValuePair<int, byte[]>(index, data));
                    Check();
                }
            }

            public async Task<List<KeyValuePair<int, byte[]>>> Take(int count)
            {
                lock (_items)
                {
                    _expected = count;
                    Check();
                }

                await _complete.Task.WaitAsync(Timeout);

                lock (_items)
                {
                    return new List<KeyValuePair<int, byte[]>>(_items);
                }
            }

            private void Check()
            {
                if (_expected >= 0 && _items.Count >= _expected)
                {
                    _complete.TrySetResult(null);
                }
            }
        }
    }
}
