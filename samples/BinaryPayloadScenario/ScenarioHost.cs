using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNet.SignalR;
using Microsoft.AspNet.SignalR.Configuration;
using Microsoft.AspNet.SignalR.Infrastructure;
using Microsoft.AspNet.SignalR.Messaging;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Owin.Builder;
using Microsoft.Owin.Security.DataProtection;
using Owin;

namespace BinaryPayloadScenario
{
    using AppFunc = Func<IDictionary<string, object>, Task>;

    /// <summary>
    /// The WebSocket messages the server exchanged with one connection, counted on the wire.
    /// </summary>
    public sealed class FrameCounts
    {
        private int _binaryFromClient;
        private int _binaryToClient;
        private int _textFromClient;
        private int _textToClient;

        public int BinaryFromClient { get { return Volatile.Read(ref _binaryFromClient); } }
        public int BinaryToClient { get { return Volatile.Read(ref _binaryToClient); } }
        public int TextFromClient { get { return Volatile.Read(ref _textFromClient); } }
        public int TextToClient { get { return Volatile.Read(ref _textToClient); } }

        internal void Received(WebSocketMessageType type)
        {
            if (type == WebSocketMessageType.Binary) Interlocked.Increment(ref _binaryFromClient);
            if (type == WebSocketMessageType.Text) Interlocked.Increment(ref _textFromClient);
        }

        internal void Sent(WebSocketMessageType type)
        {
            if (type == WebSocketMessageType.Binary) Interlocked.Increment(ref _binaryToClient);
            if (type == WebSocketMessageType.Text) Interlocked.Increment(ref _textToClient);
        }
    }

    /// <summary>
    /// Runs three SignalR servers in this process, on Kestrel through the OWIN bridge:
    /// /signalr with the default message bus and /scaleout with a backplane, both with the hook registered,
    /// and /plain without the hook.
    /// </summary>
    public sealed class ScenarioHost : IAsyncDisposable
    {
        private readonly ConcurrentDictionary<string, FrameCounts> _frames = new ConcurrentDictionary<string, FrameCounts>();
        private readonly ConcurrentDictionary<string, WebSocket> _sockets = new ConcurrentDictionary<string, WebSocket>();
        private WebApplication _app;

        public string Url { get; private set; }

        public MessagePackPayloadSerializer ServerHook { get; private set; }

        public MessagePackPayloadSerializer ScaleoutServerHook { get; private set; }

        public IDependencyResolver Resolver { get; private set; }

        public FrameCounts FramesFor(string connectionToken)
        {
            return _frames.GetOrAdd(connectionToken, _ => new FrameCounts());
        }

        /// <summary>
        /// Cuts the WebSocket of a connection the way a network failure does, the client reconnects.
        /// </summary>
        public void Drop(string connectionToken)
        {
            _sockets[connectionToken].Abort();
        }

        public static async Task<ScenarioHost> Start()
        {
            var host = new ScenarioHost();
            await host.StartCore();
            return host;
        }

        private async Task StartCore()
        {
            ServerHook = new MessagePackPayloadSerializer();
            ScaleoutServerHook = new MessagePackPayloadSerializer();

            // This is all an application does to turn the feature on
            var resolver = new DefaultDependencyResolver();
            resolver.Register(typeof(Microsoft.AspNet.SignalR.Json.IBinaryPayloadSerializer), () => ServerHook);
            // The default limit for incoming WebSocket messages is 64KB, the large payload scenario needs more
            resolver.Resolve<IConfigurationManager>().MaxIncomingWebSocketMessageSize = null;
            Resolver = resolver;

            var scaleoutResolver = new DefaultDependencyResolver();
            scaleoutResolver.Register(typeof(Microsoft.AspNet.SignalR.Json.IBinaryPayloadSerializer), () => ScaleoutServerHook);
            var backplane = new Lazy<LoopbackBackplane>(() => new LoopbackBackplane(scaleoutResolver));
            scaleoutResolver.Register(typeof(IMessageBus), () => backplane.Value);

            var builder = WebApplication.CreateBuilder();
            builder.Logging.SetMinimumLevel(LogLevel.Warning);
            builder.WebHost.UseUrls("http://127.0.0.1:0");
            // SignalR writes to the response synchronously
            builder.WebHost.ConfigureKestrel(options => options.AllowSynchronousIO = true);

            _app = builder.Build();
            _app.UseWebSockets();

            // Count what really goes over each WebSocket
            _app.Use(async (HttpContext context, RequestDelegate next) =>
            {
                var feature = context.Features.Get<IHttpWebSocketFeature>();

                if (feature != null && feature.IsWebSocketRequest)
                {
                    string token = context.Request.Query["connectionToken"];
                    context.Features.Set<IHttpWebSocketFeature>(new CountingWebSocketFeature(feature, FramesFor(token), socket => _sockets[token] = socket));
                }

                await next(context);
            });

            _app.UseOwin(pipeline => pipeline(next =>
            {
                var owin = new AppBuilder();
                owin.Properties["builder.DefaultApp"] = next;
                owin.Properties["host.AppName"] = "BinaryPayloadScenario";
                // The default is DPAPI, which only exists on Windows
                owin.SetDataProtectionProvider(new NoDataProtection());

                // The bridge doesn't announce WebSockets the way SignalR looks for them, and negotiation asks too
                owin.Use(new Func<AppFunc, AppFunc>(inner => environment =>
                {
                    environment["server.Capabilities"] = new Dictionary<string, object> { { "websocket.Version", "1.0" } };

                    return inner(environment);
                }));

                owin.MapSignalR("/signalr", new HubConfiguration { Resolver = resolver, EnableDetailedErrors = true });
                owin.MapSignalR("/scaleout", new HubConfiguration { Resolver = scaleoutResolver, EnableDetailedErrors = true });
                owin.MapSignalR("/plain", new HubConfiguration { Resolver = new DefaultDependencyResolver(), EnableDetailedErrors = true });

                return owin.Build<AppFunc>();
            }));

            await _app.StartAsync();

            Url = _app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>().Addresses.First();
        }

        public async ValueTask DisposeAsync()
        {
            using (var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3)))
            {
                await _app.StopAsync(timeout.Token);
            }

            await _app.DisposeAsync();
        }

        /// <summary>
        /// A backplane that does what Redis, SQL Server and Service Bus do with a message, without leaving the process:
        /// the messages are serialized, and what the server delivers is what it reads back.
        /// </summary>
        private sealed class LoopbackBackplane : ScaleoutMessageBus
        {
            private long _id;

            public LoopbackBackplane(IDependencyResolver resolver)
                : base(resolver, new ScaleoutConfiguration())
            {
                Open(0);
            }

            protected override Task Send(int streamIndex, IList<Message> messages)
            {
                byte[] wire = new ScaleoutMessage(messages).ToBytes();

                OnReceived(streamIndex, (ulong)Interlocked.Increment(ref _id), ScaleoutMessage.FromBytes(wire));

                return Task.CompletedTask;
            }
        }

        private sealed class NoDataProtection : IDataProtectionProvider, IDataProtector
        {
            public IDataProtector Create(params string[] purposes) { return this; }
            public byte[] Protect(byte[] userData) { return userData; }
            public byte[] Unprotect(byte[] protectedData) { return protectedData; }
        }

        private sealed class CountingWebSocketFeature : IHttpWebSocketFeature
        {
            private readonly IHttpWebSocketFeature _inner;
            private readonly FrameCounts _counts;
            private readonly Action<WebSocket> _accepted;

            public CountingWebSocketFeature(IHttpWebSocketFeature inner, FrameCounts counts, Action<WebSocket> accepted)
            {
                _inner = inner;
                _counts = counts;
                _accepted = accepted;
            }

            public bool IsWebSocketRequest { get { return _inner.IsWebSocketRequest; } }

            public async Task<WebSocket> AcceptAsync(WebSocketAcceptContext context)
            {
                var socket = new CountingWebSocket(await _inner.AcceptAsync(context), _counts);
                _accepted(socket);
                return socket;
            }
        }

        private sealed class CountingWebSocket : WebSocket
        {
            private readonly WebSocket _inner;
            private readonly FrameCounts _counts;

            public CountingWebSocket(WebSocket inner, FrameCounts counts)
            {
                _inner = inner;
                _counts = counts;
            }

            public override WebSocketCloseStatus? CloseStatus { get { return _inner.CloseStatus; } }
            public override string CloseStatusDescription { get { return _inner.CloseStatusDescription; } }
            public override WebSocketState State { get { return _inner.State; } }
            public override string SubProtocol { get { return _inner.SubProtocol; } }

            public override void Abort() { _inner.Abort(); }
            public override void Dispose() { _inner.Dispose(); }

            public override Task CloseAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
            {
                return _inner.CloseAsync(closeStatus, statusDescription, cancellationToken);
            }

            public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string statusDescription, CancellationToken cancellationToken)
            {
                return _inner.CloseOutputAsync(closeStatus, statusDescription, cancellationToken);
            }

            public override async Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
            {
                WebSocketReceiveResult result = await _inner.ReceiveAsync(buffer, cancellationToken);

                if (result.EndOfMessage)
                {
                    _counts.Received(result.MessageType);
                }

                return result;
            }

            public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
            {
                if (endOfMessage)
                {
                    // Counted before it is sent, so the count is there by the time the client has the message
                    _counts.Sent(messageType);
                }

                return _inner.SendAsync(buffer, messageType, endOfMessage, cancellationToken);
            }
        }
    }
}
