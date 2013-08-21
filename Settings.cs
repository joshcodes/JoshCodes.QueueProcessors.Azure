using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace JoshCodes.QueueProcessors.Azure
{
    internal static class Settings
    {
        internal static string ServiceBusConnectionString
        {
            get
            {
                var serviceBusConnectionString = System.Configuration.ConfigurationManager.AppSettings["azure.service_bus-connection_string"];
                // "Endpoint=sb://magicmoments-api.servicebus.windows.net/;SharedSecretIssuer=owner;SharedSecretValue=4AcW4dp63TJoEtm8WOwlMNscRpvzChhheB6Qzq+CobA=";
                //  Microsoft.WindowsAzure.CloudConfigurationManager.GetSetting("Microsoft.ServiceBus.ConnectionString");
                return serviceBusConnectionString;
            }
        }
    }
}
