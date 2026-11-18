# BinaryPayloadScenario

End-to-end scenarios for `IBinaryPayloadSerializer`: hub arguments, return values and client method arguments that a
hook turns into bytes are sent as binary WebSocket messages instead of base64 inside the JSON.

```
dotnet run --project samples/BinaryPayloadScenario
```

It starts a server in the process, connects real clients to it over WebSockets, server-sent events and long polling,
and prints `PASS` or `FAIL` per scenario. The exit code is the number of failed scenarios.

## What it checks

The hook is `MessagePackPayloadSerializer` in `ScenarioHub.cs`: it takes `byte[]` values as they are and serializes
`[MessagePackObject]` values with MessagePack. The same class is registered on the server and set on the client.

| Group | Scenarios |
|---|---|
| Client to server | object argument bound to a `byte[]` parameter, binary and JSON arguments mixed, an invocation with nothing to take, a 1MB payload, `AutoTransport`, a client without a hook |
| Server to client | return value, client method invoked from a hub, two payloads in one invocation, broadcast from outside a hub, broadcast from a hub, send to several connections, 200 messages at once |
| Other transports and scaleout | server-sent events and long polling keep the JSON form, a backplane delivers base64 |
| Concurrency and delivery | concurrent invocations on one connection, publishing from many threads, responses that leave out messages for the caller, messages published while a client reconnects (and a buffer reused after the send) |
| Values | empty payload, null, asynchronous result, text outside ASCII, a binary argument for a parameter that is not a `byte[]`, progress values, an invocation on a stopped connection |
| The reserved value | `{"$binary":true}` is refused where it is sent (by the client, and by the server when it publishes), values that only look similar are ordinary JSON, a hub state with the value next to a binary result, a server without the hook |
| Wire format | the layout of a binary message checked without the SignalR client, a truncated binary message, payloads that don't match the placeholders, a client that only sent text |

## What an application has to know

- `{"$binary":true}` stands for a binary value in the JSON. A value that is serialized as exactly that can't be sent
  once a hook is registered: the client refuses the invocation, the server refuses to publish, both with an
  `InvalidOperationException`. Without a hook nothing changes.
- Binary messages are not negotiated. With a hook on the server every WebSocket client has to be a client that
  understands them, with a hook on the client so does the server.
- The hook is only used over WebSockets. Over the other transports a pushed value the hook takes arrives as base64,
  while arguments and results keep their JSON form.

Whether a message was binary is not taken from the client: the server counts the WebSocket messages of every
connection on the wire (`CountingWebSocket` in `ScenarioHost.cs`), and each scenario asserts how many binary ones it added.

## How it is hosted

`Microsoft.AspNet.SignalR.Core` only targets .NET Framework. Here it is loaded as it is on .NET and runs on Kestrel
through `Microsoft.AspNetCore.Owin`, so the scenarios run with the .NET SDK alone on Windows, macOS and Linux.
`ScenarioHost.cs` has the three things that were needed to make that work (data protection, synchronous IO, the WebSocket
capability). It is a way to run the scenarios, not a supported way to host the server.

The project has its own empty `Directory.Build.props` and `Directory.Build.targets` so that it doesn't pick up the
build settings of the repository, and it is not part of `Microsoft.AspNet.SignalR.sln`.
