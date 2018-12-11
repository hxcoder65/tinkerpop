#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Driver.Messages;
using Gremlin.Net.Structure.IO.GraphSON;
using Newtonsoft.Json.Linq;

namespace Gremlin.Net.Driver
{
    internal interface IResponseHandlerForSingleRequestMessage
    {
        void HandleReceived(ResponseMessage<JToken> received);
        void Finalize(Dictionary<string, object> statusAttributes);
        void HandleFailure(Exception objException);
    }

    internal class Connection : IConnection
    {
        private readonly GraphSONReader _graphSONReader;
        private readonly GraphSONWriter _graphSONWriter;
        private readonly JsonMessageSerializer _messageSerializer;
        private readonly Uri _uri;
        private readonly WebSocketConnection _webSocketConnection;
        private readonly string _username;
        private readonly string _password;
        private readonly ConcurrentQueue<RequestMessage> _writeQueue = new ConcurrentQueue<RequestMessage>();

        private readonly ConcurrentDictionary<Guid, IResponseHandlerForSingleRequestMessage> _callbackByRequestId =
            new ConcurrentDictionary<Guid, IResponseHandlerForSingleRequestMessage>();
        private int _connectionState = 0;
        private int _writeInProgress = 0;
        private const int Closed = 1;

        public Connection(Uri uri, string username, string password, GraphSONReader graphSONReader,
            GraphSONWriter graphSONWriter, string mimeType, Action<ClientWebSocketOptions> webSocketConfiguration)
        {
            _uri = uri;
            _username = username;
            _password = password;
            _graphSONReader = graphSONReader;
            _graphSONWriter = graphSONWriter;
            _messageSerializer = new JsonMessageSerializer(mimeType);
            _webSocketConnection = new WebSocketConnection(webSocketConfiguration);
        }

        public async Task ConnectAsync()
        {
            await _webSocketConnection.ConnectAsync(_uri).ConfigureAwait(false);
            ReceiveNext();
        }

        public int NrRequestsInFlight => _callbackByRequestId.Count;

        public bool IsOpen => _webSocketConnection.IsOpen;

        public Task<ResultSet<T>> SubmitAsync<T>(RequestMessage requestMessage)
        {
            var receiver = new ResponseHandlerForSingleRequestMessage<T>(_graphSONReader);
            _callbackByRequestId.GetOrAdd(requestMessage.RequestId, receiver);
            _writeQueue.Enqueue(requestMessage);
            SendNextMessage();
            return receiver.Result;
        }

        private void ReceiveNext()
        {
            var state = Interlocked.CompareExchange(ref _connectionState, 0, 0);
            if (state == Closed) return;
            _webSocketConnection.ReceiveMessageAsync().ContinueWith(
                received =>
                {
                    Parse(received.Result);
                    ReceiveNext();
                },
                TaskContinuationOptions.ExecuteSynchronously);
        }

        private void Parse(byte[] received)
        {
            var receivedMsg = _messageSerializer.DeserializeMessage<ResponseMessage<JToken>>(received);

            var status = receivedMsg.Status;
            if (status.Code.IndicatesError())
            {
                _callbackByRequestId[receivedMsg.RequestId].HandleFailure(new ResponseException(status.Code,
                    status.Attributes, $"{status.Code}: {status.Message}"));
                _callbackByRequestId.TryRemove(receivedMsg.RequestId, out _);
                return;
            }

            if (status.Code == ResponseStatusCode.Authenticate)
            {
                Authenticate(receivedMsg.RequestId);
                return;
            }

            if (status.Code != ResponseStatusCode.NoContent)
                _callbackByRequestId[receivedMsg.RequestId].HandleReceived(receivedMsg);

            if (status.Code == ResponseStatusCode.Success || status.Code == ResponseStatusCode.NoContent)
            {
                _callbackByRequestId[receivedMsg.RequestId].Finalize(status.Attributes);
                _callbackByRequestId.TryRemove(receivedMsg.RequestId, out _);
            }
        }
        
        private void Authenticate(Guid requestId)
        {
            if (string.IsNullOrEmpty(_username) || string.IsNullOrEmpty(_password))
            {
                _callbackByRequestId[requestId].HandleFailure(new InvalidOperationException(
                    $"The Gremlin Server requires authentication, but no credentials are specified - username: {_username}, password: {_password}."));
                _callbackByRequestId.TryRemove(requestId, out _);
                return;
            }

            var message = RequestMessage.Build(Tokens.OpsAuthentication).Processor(Tokens.ProcessorTraversal)
                .AddArgument(Tokens.ArgsSasl, SaslArgument()).Create();

            _writeQueue.Enqueue(message);
            SendNextMessage();
        }

        private string SaslArgument()
        {
            var auth = $"\0{_username}\0{_password}";
            var authBytes = Encoding.UTF8.GetBytes(auth);
            return Convert.ToBase64String(authBytes);
        }

        private void SendNextMessage()
        {
            if (Interlocked.CompareExchange(ref _writeInProgress, 1, 0) != 0)
                return;
            if (_writeQueue.TryDequeue(out var msg))
                SendMessageAsync(msg).ContinueWith(
                    t =>
                    {
                        if (t.IsFaulted)
                        {
                            _callbackByRequestId[msg.RequestId].HandleFailure(t.Exception);
                            _callbackByRequestId.TryRemove(msg.RequestId, out _);
                        }

                        SendNextMessage();
                    }, TaskContinuationOptions.ExecuteSynchronously);
            Interlocked.CompareExchange(ref _writeInProgress, 0, 1);
        }
        
        private async Task SendMessageAsync(RequestMessage message)
        {
            var graphsonMsg = _graphSONWriter.WriteObject(message);
            var serializedMsg = _messageSerializer.SerializeMessage(graphsonMsg);
            await _webSocketConnection.SendMessageAsync(serializedMsg).ConfigureAwait(false);
        }

        public async Task CloseAsync()
        {
            Interlocked.Exchange(ref _connectionState, Closed);
            await _webSocketConnection.CloseAsync().ConfigureAwait(false);
        }

        #region IDisposable Support

        private bool _disposed;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                    _webSocketConnection?.Dispose();
                _disposed = true;
            }
        }

        #endregion
    }
}