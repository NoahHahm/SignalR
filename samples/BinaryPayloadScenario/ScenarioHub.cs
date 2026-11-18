using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MessagePack;
using Microsoft.AspNet.SignalR;
using Newtonsoft.Json.Linq;

namespace BinaryPayloadScenario
{
    /// <summary>
    /// The payload of the scenarios. Anything marked with [MessagePackObject] is sent as raw MessagePack bytes.
    /// </summary>
    [MessagePackObject]
    public class PlayerState
    {
        [Key(0)]
        public int Id { get; set; }

        [Key(1)]
        public string Name { get; set; }

        [Key(2)]
        public float[] Position { get; set; }

        [Key(3)]
        public byte[] Blob { get; set; }

        public static PlayerState Create(int id, string name, int blobSize = 16)
        {
            var blob = new byte[blobSize];
            new Random(id).NextBytes(blob);

            return new PlayerState { Id = id, Name = name, Position = new[] { id, id * 0.5f, -id }, Blob = blob };
        }

        public byte[] Pack()
        {
            return MessagePackSerializer.Serialize(this);
        }

        public static PlayerState Unpack(byte[] data)
        {
            return MessagePackSerializer.Deserialize<PlayerState>(data);
        }
    }

    /// <summary>
    /// The hook under test, the way an application would write it: one class for the server and the client.
    /// It takes byte[] values as they are and serializes [MessagePackObject] values with MessagePack.
    /// The receiver always gets a byte[].
    /// </summary>
    public sealed class MessagePackPayloadSerializer :
        Microsoft.AspNet.SignalR.Json.IBinaryPayloadSerializer,
        Microsoft.AspNet.SignalR.Client.IBinaryPayloadSerializer
    {
        private int _taken;

        /// <summary>
        /// The number of values this instance has taken so far.
        /// </summary>
        public int Taken
        {
            get { return Volatile.Read(ref _taken); }
        }

        public bool TrySerialize(object value, out byte[] payload)
        {
            payload = value as byte[];

            if (payload == null && value.GetType().IsDefined(typeof(MessagePackObjectAttribute), inherit: false))
            {
                payload = MessagePackSerializer.Serialize(value.GetType(), value);
            }

            if (payload == null)
            {
                return false;
            }

            Interlocked.Increment(ref _taken);
            return true;
        }
    }

    public class ScenarioHub : Hub
    {
        // Binary argument, JSON result
        public int Length(byte[] data)
        {
            return data.Length;
        }

        // JSON arguments, binary result (the hook serializes the returned object)
        public PlayerState Make(int id, string name)
        {
            return PlayerState.Create(id, name);
        }

        // Binary argument, binary result
        public byte[] Echo(byte[] data)
        {
            return data;
        }

        // Binary and JSON arguments mixed
        public string Mixed(string text, byte[] first, int number, byte[] second)
        {
            return text + "|" + PlayerState.Unpack(first).Name + "|" + number + "|" + PlayerState.Unpack(second).Name;
        }

        // Nothing for the hook to take
        public string Plain(string text)
        {
            return text.ToUpperInvariant();
        }

        // The JSON form of the payload, what transports other than WebSockets keep using
        public PlayerState Json(PlayerState state)
        {
            return state;
        }

        public Task PushToCaller(string tag, byte[] data)
        {
            return Clients.Caller.onState(tag, PlayerState.Unpack(data));
        }

        public Task PushPairToCaller(string tag, byte[] first, byte[] second)
        {
            return Clients.Caller.onPair(tag, PlayerState.Unpack(first), "between", PlayerState.Unpack(second));
        }

        public Task PushToAll(string tag, byte[] data)
        {
            return Clients.All.onState(tag, PlayerState.Unpack(data));
        }

        // Several signals at once, which is published differently from a single one
        public Task PushToMany(string tag, IList<string> connectionIds, byte[] data)
        {
            return Clients.Clients(connectionIds).onState(tag, PlayerState.Unpack(data));
        }

        public async Task<PlayerState> MakeAsync(int id, string name)
        {
            await Task.Yield();
            return PlayerState.Create(id, name);
        }

        public PlayerState Nothing()
        {
            return null;
        }

        // A JSON value that looks like the placeholder of a binary value, next to a binary one
        public string Flags(IDictionary<string, bool> flags, byte[] data)
        {
            return String.Join(",", flags.Keys) + "|" + PlayerState.Unpack(data).Name;
        }

        // A JSON value of any shape next to a payload, or without one when data is null
        public Task PushShape(string tag, string shape, byte[] data)
        {
            return Clients.Caller.onFlags(tag, JToken.Parse(shape), data);
        }

        // A binary result and, in the same response, a hub state that looks like the placeholder of a binary value
        public PlayerState MakeWithState(int id, string name)
        {
            Clients.Caller.flags = new Dictionary<string, bool> { { "$binary", true } };
            return PlayerState.Create(id, name);
        }

        // What the arguments were bound to
        public string Shapes(object first, object second)
        {
            return first.GetType().Name + "|" + second.GetType().Name;
        }

        // The value that looks like a placeholder is in a message of its own, the binary values are in other messages
        public Task BurstWithFlags(string tag, int count)
        {
            var tasks = new List<Task>();

            for (int i = 0; i < count; i++)
            {
                tasks.Add(Clients.Caller.onFlags(tag + "-flags", new Dictionary<string, bool> { { "$binary", false } }, "text"));
                tasks.Add(Clients.Caller.onBurst(tag, i, PlayerState.Create(i, "burst-" + i)));
            }

            return Task.WhenAll(tasks);
        }

        public Task PushTextToCaller(string tag, string text, byte[] data)
        {
            PlayerState state = PlayerState.Unpack(data);
            return Clients.Caller.onPair(tag, state, text, state);
        }

        public async Task<int> Report(int count, IProgress<PlayerState> progress)
        {
            for (int i = 0; i < count; i++)
            {
                progress.Report(PlayerState.Create(i, "progress-" + i));
                await Task.Yield();
            }

            return count;
        }

        // Every second message leaves the caller out, so a response for the caller skips messages that carry a payload
        public Task BurstOthers(string tag, int count)
        {
            var tasks = new Task[count];

            for (int i = 0; i < count; i++)
            {
                PlayerState state = PlayerState.Create(i, "burst-" + i);
                tasks[i] = i % 2 == 0 ? Clients.All.onBurst(tag, i, state) : Clients.Others.onBurst(tag, i, state);
            }

            return Task.WhenAll(tasks);
        }

        // Published from many threads at once
        public Task Storm(string tag, int count)
        {
            dynamic all = Clients.All;

            return Task.WhenAll(Enumerable.Range(0, count).Select(i => Task.Run(() => (Task)all.onBurst(tag, i, PlayerState.Create(i, "burst-" + i)))));
        }

        // Published without waiting in between, so that several messages end up in one response
        public Task Burst(string tag, int count)
        {
            var tasks = new Task[count];

            for (int i = 0; i < count; i++)
            {
                tasks[i] = Clients.All.onBurst(tag, i, PlayerState.Create(i, "burst-" + i));
            }

            return Task.WhenAll(tasks);
        }
    }
}
