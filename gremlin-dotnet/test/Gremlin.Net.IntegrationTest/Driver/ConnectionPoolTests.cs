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
using System.Collections.Generic;
using System.Threading.Tasks;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Exceptions;
using Gremlin.Net.IntegrationTest.Util;
using Xunit;

namespace Gremlin.Net.IntegrationTest.Driver
{
    public class ConnectionPoolTests
    {
        private readonly RequestMessageProvider _requestMessageProvider = new RequestMessageProvider();
        private static readonly string TestHost = ConfigProvider.Configuration["TestServerIpAddress"];
        private static readonly int TestPort = Convert.ToInt32(ConfigProvider.Configuration["TestServerPort"]);

        private async Task ExecuteMultipleLongRunningRequestsInParallel(IGremlinClient gremlinClient, int nrRequests,
            int requestRunningTimeInMs)
        {
            var tasks = new List<Task>(nrRequests);
            for (var i = 0; i < nrRequests; i++)
            {
                tasks.Add(gremlinClient.SubmitAsync(_requestMessageProvider.GetSleepMessage(requestRunningTimeInMs)));
            }
                
            await Task.WhenAll(tasks);
        }

        [Fact]
        public async Task ShouldReuseConnectionForSequentialRequests()
        {
            const int minConnectionPoolSize = 1;
            using (var gremlinClient = CreateGremlinClient(minConnectionPoolSize))
            {
                await gremlinClient.SubmitAsync("");
                await gremlinClient.SubmitAsync("");

                var nrConnections = gremlinClient.NrConnections;
                Assert.Equal(1, nrConnections);
            }
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(8)]
        public void ShouldStartWithConfiguredNrMinConnections(int minConnectionPoolSize)
        {
            using (var gremlinClient = CreateGremlinClient(minConnectionPoolSize))
            {
                var nrConnections = gremlinClient.NrConnections;
                Assert.Equal(minConnectionPoolSize, nrConnections);
            }
        }

        [Fact]
        public async Task ShouldExecuteParallelRequestsOnDifferentConnections()
        {
            const int nrBlockingTasks = 1;
            const int nrFastTasks = 1;
            using (var gremlinClient = CreateGremlinClient(nrBlockingTasks + nrFastTasks, nrBlockingTasks + nrFastTasks))
            {
                const int sleepTime = 200;

                var blockingConnectionTask =
                    ExecuteMultipleLongRunningRequestsInParallel(gremlinClient, nrBlockingTasks, sleepTime);
                await Task.Delay(50);
                var fastTask = ExecuteMultipleFastRequestsInParallel(gremlinClient, nrFastTasks);

                var completedTask =
                    await WaitForTaskOrTimeoutAsync(fastTask, TimeSpan.FromMilliseconds(sleepTime - 10));
                await fastTask;
                await blockingConnectionTask;
                Assert.Equal(fastTask, completedTask);
            }
        }

        private async Task ExecuteMultipleFastRequestsInParallel(IGremlinClient gremlinClient, int nrRequests)
        {
            var tasks = new List<Task>(nrRequests);
            for (var i = 0; i < nrRequests; i++)
            {
                tasks.Add(gremlinClient.SubmitAsync(_requestMessageProvider.GetDummyMessage()));
            }
                
            await Task.WhenAll(tasks);
        }

        [Fact]
        public async Task ShouldNotCreateMoreThanConfiguredNrMaxConnections()
        {
            const int maxConnectionPoolSize = 1;
            using (var gremlinClient = CreateGremlinClient(maxConnectionPoolSize, maxConnectionPoolSize, 2))
            {
                const int sleepTime = 100;

                await ExecuteMultipleLongRunningRequestsInParallel(gremlinClient, maxConnectionPoolSize * 2, sleepTime);

                Assert.True(gremlinClient.NrConnections <= maxConnectionPoolSize);
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        public async Task ShouldCreateNewConnectionWhenNoConnectionIsAvailable(int nrParallelRequests)
        {
            using (var gremlinClient = CreateGremlinClient(minConnectionPoolSize: nrParallelRequests - 1,
                maxConnectionPoolSize: nrParallelRequests, maxInProcessPerConnection: 1))
            {
                const int sleepTime = 100;

                await ExecuteMultipleLongRunningRequestsInParallel(gremlinClient, nrParallelRequests, sleepTime);

                Assert.Equal(nrParallelRequests, gremlinClient.NrConnections);
            }
        }

        [Fact]
        public async Task ShouldThrowNoConnectionAvailableExceptionWhenNoConnectionIsAvailable()
        {
            const int nrParallelRequests = 2;
            using (var gremlinClient = CreateGremlinClient(minConnectionPoolSize: 1, maxConnectionPoolSize: 1,
                maxInProcessPerConnection: 1))
            {
                const int sleepTime = 100;

                await Assert.ThrowsAsync<NoConnectionAvailableException>(() =>
                    ExecuteMultipleLongRunningRequestsInParallel(gremlinClient, nrParallelRequests, sleepTime));
            }
        }

        private static GremlinClient CreateGremlinClient(int minConnectionPoolSize = 0, int maxConnectionPoolSize = 8,
            int maxInProcessPerConnection = 4)
        {
            var gremlinServer = new GremlinServer(TestHost, TestPort);
            return new GremlinClient(gremlinServer,
                connectionPoolSettings: new ConnectionPoolSettings
                {
                    MinSize = minConnectionPoolSize,
                    MaxSize = maxConnectionPoolSize,
                    MaxInProcessPerConnection = maxInProcessPerConnection
                });
        }

        private static Task<Task> WaitForTaskOrTimeoutAsync(Task task, TimeSpan timeout)
        {
            return Task.WhenAny(task, Task.Delay(timeout));
        }
    }
}