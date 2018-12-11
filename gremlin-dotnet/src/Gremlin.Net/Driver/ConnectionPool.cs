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
using System.Threading;
using System.Threading.Tasks;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.Process;

namespace Gremlin.Net.Driver
{
    internal class ConnectionPool : IDisposable
    {
        private readonly ConnectionFactory _connectionFactory;
        private readonly ConcurrentBag<Connection> _connections = new ConcurrentBag<Connection>();
        private readonly int _minPoolSize;
        private readonly int _maxPoolSize;
        private readonly int _maxInProcessPerConnection;
        private int _nrConnections;

        public ConnectionPool(ConnectionFactory connectionFactory, ConnectionPoolSettings settings)
        {
            _connectionFactory = connectionFactory;
            _minPoolSize = settings.MinSize;
            _maxPoolSize = settings.MaxSize;
            _maxInProcessPerConnection = settings.MaxInProcessPerConnection;
            PopulatePoolAsync().WaitUnwrap();
        }

        public int NrConnections => Interlocked.CompareExchange(ref _nrConnections, 0, 0);

        private async Task PopulatePoolAsync()
        {
            var connectionCreationTasks = new List<Task<Connection>>(_minPoolSize);
            for (var i = 0; i < _minPoolSize; i++)
            {
                connectionCreationTasks.Add(CreateNewConnectionAsync());
            }

            var createdConnections = await Task.WhenAll(connectionCreationTasks).ConfigureAwait(false);
            foreach (var c in createdConnections)
            {
                _connections.Add(c);
            }

            Interlocked.CompareExchange(ref _nrConnections, _minPoolSize, 0);
        }

        public async Task<IConnection> GetAvailableConnectionAsync()
        {
            if (TryGetConnectionFromPool(out var connection))
                return ProxiedConnection(connection);
            connection = await AddConnectionIfUnderMaximumAsync().ConfigureAwait(false);
            if (connection == null)
                throw new NoConnectionAvailableException("no connection available!");
            return ProxiedConnection(connection);
        }

        private IConnection ProxiedConnection(Connection connection)
        {
            return new ProxyConnection(connection, ReturnConnectionIfOpen);
        }

        private void ReturnConnectionIfOpen(Connection connection)
        {
            if (connection.IsOpen) return;
            ConsiderUnavailable();
            DefinitelyDestroyConnection(connection);
        }

        private async Task<Connection> AddConnectionIfUnderMaximumAsync()
        {
            while (true)
            {
                var nrOpened = Interlocked.CompareExchange(ref _nrConnections, 0, 0);
                if (nrOpened >= _maxPoolSize) return null;

                if (Interlocked.CompareExchange(ref _nrConnections, nrOpened + 1, nrOpened) == nrOpened)
                    break;
            }

            var connection = await CreateNewConnectionAsync().ConfigureAwait(false);
            _connections.Add(connection);
            return connection;
        }

        private async Task<Connection> CreateNewConnectionAsync()
        {
            var newConnection = _connectionFactory.CreateConnection();
            await newConnection.ConnectAsync().ConfigureAwait(false);
            return newConnection;
        }

        private bool TryGetConnectionFromPool(out Connection connection)
        {
            while (true)
            {
                connection = SelectLeastUsedConnection();
                if (connection == null) return false; // _connections is empty
                if (connection.NrRequestsInFlight >= _maxInProcessPerConnection)
                    return false;
                if (connection.IsOpen) return true;
                DefinitelyDestroyConnection(connection);
            }
        }

        private Connection SelectLeastUsedConnection()
        {
            if (_connections.IsEmpty) return null;
            var nrMinInFlightConnections = int.MaxValue;
            Connection leastBusy = null;
            foreach (var connection in _connections)
            {
                var nrInFlight = connection.NrRequestsInFlight;
                if (nrInFlight >= nrMinInFlightConnections) continue;
                nrMinInFlightConnections = nrInFlight;
                leastBusy = connection;
            }

            return leastBusy;
        }

        private void ConsiderUnavailable()
        {
            CloseAndRemoveAllConnectionsAsync().WaitUnwrap();
        }

        private async Task CloseAndRemoveAllConnectionsAsync()
        {
            while (_connections.TryTake(out var connection))
            {
                await connection.CloseAsync().ConfigureAwait(false);
                DefinitelyDestroyConnection(connection);
            }
        }

        private void DefinitelyDestroyConnection(Connection connection)
        {
            connection.Dispose();
            Interlocked.Decrement(ref _nrConnections);
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
                    CloseAndRemoveAllConnectionsAsync().WaitUnwrap();
                _disposed = true;
            }
        }

        #endregion
    }
}