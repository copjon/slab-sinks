using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using FullScale180.SemanticLogging.Properties;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Sinks;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging.Utility;
using Newtonsoft.Json.Linq;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.ApplicationInsights.Channel;
using Microsoft.ApplicationInsights.DataContracts;
using Newtonsoft.Json;

namespace FullScale180.SemanticLogging.Sinks
{
    /// <summary>
    /// Sink that asynchronously writes entries to a Elasticsearch server.
    /// </summary>
    public class ApplicationInsightsSink : IObserver<EventEntry>, IDisposable
    {
        private const string BulkServiceOperationPath = "_bulk";

        private readonly BufferedEventPublisher<EventEntry> bufferedPublisher;
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        
        private readonly string instanceName;
        
        private readonly TelemetryClient telemetryClient;
        private readonly TimeSpan onCompletedTimeout;
        private readonly Dictionary<string, string> globalContext;

        /// <summary>
        /// Initializes a new instance of the <see cref="ApplicationInsightsSink"/> class with the specified instrumentation key.
        /// </summary>
        /// <param name="instanceName">The name of the instance originating the entries.</param>
        /// <param name="telemetryConfiguration">The the TelemetryConfiguration for application insights.</param>
        /// <param name="bufferInterval">The buffering interval to wait for events to accumulate before sending them to Elasticsearch.</param>
        /// <param name="bufferingCount">The buffering event entry count to wait before sending events to Elasticsearch </param>
        /// <param name="maxBufferSize">The maximum number of entries that can be buffered while it's sending to Windows Azure Storage before the sink starts dropping entries.</param>
        /// <param name="onCompletedTimeout">Defines a timeout interval for when flushing the entries after an <see cref="OnCompleted"/> call is received and before disposing the sink.
        /// This means that if the timeout period elapses, some event entries will be dropped and not sent to the store. Normally, calling <see cref="IDisposable.Dispose"/> on 
        /// the <see cref="System.Diagnostics.Tracing.EventListener"/> will block until all the entries are flushed or the interval elapses.
        /// If <see langword="null"/> is specified, then the call will block indefinitely until the flush operation finishes.</param>
        /// <param name="globalContext">A set of global environment parameters to be included in each log entry</param>
        public ApplicationInsightsSink(string instanceName,TelemetryConfiguration telemetryConfiguration, TimeSpan bufferInterval,
            int bufferingCount, int maxBufferSize, TimeSpan onCompletedTimeout, Dictionary<string, string> globalContext = null)
        {
            Guard.ArgumentNotNullOrEmpty(instanceName, "instanceName");
            Guard.ArgumentNotNull(telemetryConfiguration, "telemetryConfiguration");
            Guard.ArgumentIsValidTimeout(onCompletedTimeout, "onCompletedTimeout");
            Guard.ArgumentGreaterOrEqualThan(0, bufferingCount, "bufferingCount");
            
            this.onCompletedTimeout = onCompletedTimeout;
            this.instanceName = instanceName;
            this.telemetryClient = new TelemetryClient(telemetryConfiguration);
            var sinkId = string.Format(CultureInfo.InvariantCulture, "ApplicationInsightsSink ({0})", instanceName);
            bufferedPublisher = BufferedEventPublisher<EventEntry>.CreateAndStart(sinkId, PublishEventsAsync, bufferInterval,
                bufferingCount, maxBufferSize, cancellationTokenSource.Token);

            this.globalContext = globalContext;
        }

        /// <summary>
        /// Releases all resources used by the current instance of the <see cref="ElasticsearchSink"/> class.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Notifies the observer that the provider has finished sending push-based notifications.
        /// </summary>
        public void OnCompleted()
        {
            FlushSafe();
            Dispose();
        }

        /// <summary>
        /// Provides the sink with new data to write.
        /// </summary>
        /// <param name="value">The current entry to write to Windows Azure.</param>
        public void OnNext(EventEntry value)
        {
            if (value == null)
            {
                return;
            }

            bufferedPublisher.TryPost(value);
        }

        /// <summary>
        /// Notifies the observer that the provider has experienced an error condition.
        /// </summary>
        /// <param name="error">An object that provides additional information about the error.</param>
        public void OnError(Exception error)
        {
            FlushSafe();
            Dispose();
        }

        /// <summary>
        /// Finalizes an instance of the <see cref="ElasticsearchSink"/> class.
        /// </summary>
        ~ApplicationInsightsSink()
        {
            Dispose(false);
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <param name="disposing">A value indicating whether or not the class is disposing.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2213:DisposableFieldsShouldBeDisposed",
            MessageId = "cancellationTokenSource", Justification = "Token is canceled")]
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                cancellationTokenSource.Cancel();
                bufferedPublisher.Dispose();                
            }
        }

        /// <summary>
        /// Causes the buffer to be written immediately.
        /// </summary>
        /// <returns>The Task that flushes the buffer.</returns>
        public Task FlushAsync()
        {
            return bufferedPublisher.FlushAsync();
        }

        internal Task<int> PublishEventsAsync(IList<EventEntry> collection)
        {
            try
            {
                foreach(var entry in collection)
                {
                    Exception exception;
                    var properties = GetEventEntryProperties(entry, out exception);

                    if (exception != null)
                    {
                        HandleExceptionEntry(entry, properties, exception);
                    }
                    else
                    {
                        HandleEntry(entry, properties);
                    }
                }
                return Task.FromResult(0);
            }
            catch (OperationCanceledException)
            {
                return Task.FromResult(0);
            }
            catch (Exception ex)
            {
                // Although this is generally considered an anti-pattern this is not logged upstream and we have context
                SemanticLoggingEventSource.Log.CustomSinkUnhandledFault(ex.ToString());
                throw;
            }
        }
        
        private Dictionary<string, string> GetEventEntryProperties(EventEntry entry, out Exception exception)
        {
            exception = null;

            if (entry == null)
            {
                return null;
            }
            var properties = new Dictionary<string, string>();
            foreach (var pair in globalContext)
            {
                properties["CTX_" + pair.Key] = pair.Value;
            }
            properties["Message"] = entry.FormattedMessage;
            properties["EventId"] = entry.EventId.ToString();
            properties["EventDate"] = entry.Timestamp.UtcDateTime.ToString();
            properties["Keywords"] = entry.Schema.Keywords.ToString();
            properties["ProviderId"] = entry.Schema.ProviderId.ToString();
            properties["ProviderName"] = entry.Schema.ProviderName;
            properties["InstanceName"] = this.instanceName;
            properties["Level"] = ((int)entry.Schema.Level).ToString();
            properties["LevelName"] = entry.Schema.Level.ToString();
            properties["Opcode"] = ((int)entry.Schema.Opcode).ToString();
            properties["Task"] = ((int)entry.Schema.Task).ToString();
            properties["Version"] = entry.Schema.Version.ToString();
            properties["ProcessId"] = entry.ProcessId.ToString();
            properties["ThreadId"] = entry.ThreadId.ToString();
            foreach (var payload in entry.Schema.Payload.Zip(entry.Payload, Tuple.Create))
            {
                if (payload.Item1.Equals("Payload__jsonPayload", StringComparison.InvariantCultureIgnoreCase))
                {
                    try
                    {
                        var jsonPayload = JsonConvert.DeserializeObject<Dictionary<string, object>>(payload.Item2.ToString());

                        foreach (var pair in jsonPayload)
                        {
                            properties[pair.Key] = pair.Value.ToString();
                        }
                        continue;
                    }
                    catch (Exception)
                    {
                        //Suppress and just write the string payload
                    }

                    try
                    {
                        properties[payload.Item1] = payload.Item2.ToString();
                    }
                    catch (Exception)
                    {
                        //Suppress and continue. This part of the message is lost.
                    }
                }
                else if (payload.Item1.Equals("Payload_exception", StringComparison.InvariantCultureIgnoreCase))
                {
                    exception = (Exception)payload.Item2;
                }
                else
                {
                    properties[payload.Item1] = payload.Item2.ToString();
                }
            }

            return properties;
        }
        private bool HandleExceptionEntry(EventEntry entry, Dictionary<string,string> properties, Exception exception)
        {
            if (entry == null)
            {
                return false;
            }
            var severityLevel = SeverityLevel.Error;
            switch (entry.Schema.Level)
            {
                case System.Diagnostics.Tracing.EventLevel.Critical:
                    severityLevel = SeverityLevel.Critical;
                    break;
                case System.Diagnostics.Tracing.EventLevel.Error:
                    severityLevel = SeverityLevel.Error;
                    break;
                case System.Diagnostics.Tracing.EventLevel.Warning:
                    severityLevel = SeverityLevel.Warning;
                    break;
                case System.Diagnostics.Tracing.EventLevel.Informational:
                    severityLevel = SeverityLevel.Information;
                    break;
                case System.Diagnostics.Tracing.EventLevel.Verbose:
                    severityLevel = SeverityLevel.Verbose;
                    break;
                case System.Diagnostics.Tracing.EventLevel.LogAlways:
                    severityLevel = SeverityLevel.Verbose;
                    break;
            }
            var exceptionTelemetry = new ExceptionTelemetry(exception)
            {
                SeverityLevel = severityLevel
            };

            foreach (var pair in properties)
            {
                exceptionTelemetry.Properties[pair.Key] = pair.Value;
            }

            telemetryClient.TrackException(exceptionTelemetry);
            return true;
        }

        private bool HandleEntry(EventEntry entry, Dictionary<string, string> properties)
        {
            if (entry == null)
            {
                return false;
            }
            
            var telemetry = new EventTelemetry(entry.FormattedMessage);
            foreach (var pair in properties)
            {
                telemetry.Properties[pair.Key] = pair.Value;
            }

            telemetryClient.TrackEvent(telemetry);

            return true;
        }

        private void FlushSafe()
        {
            try
            {
                FlushAsync().Wait(onCompletedTimeout);
            }
            catch (AggregateException ex)
            {
                // Flush operation will already log errors. Never expose this exception to the observable.
                ex.Handle(e => e is FlushFailedException);
            }
        }
    }
}