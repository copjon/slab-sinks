using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.Practices.EnterpriseLibrary.SemanticLogging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SemanticLogging.ApplicationInsights.Utility
{
    public class ApplicationInsightsEventEntryUtility
    {
        private readonly string instanceName;
        private readonly Dictionary<string, string> globalContext;
        public ApplicationInsightsEventEntryUtility(string instanceName, Dictionary<string, string> globalContext)
        {
            this.instanceName = instanceName;
            this.globalContext = globalContext;
        }

        public IEnumerable<EventTelemetry> HandleTelemetry(IEnumerable<EventEntry> entries)
        {
            if (entries == null)
            {
                return null;
            }

            return entries.Select(EntryToTelemetry);
        }

    }
}
