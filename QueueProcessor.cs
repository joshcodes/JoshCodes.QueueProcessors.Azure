using System;
using System.Net;
using System.Collections.Generic;

using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;

using JoshCodes.Collections.Generic;

namespace JoshCodes.QueueProcessors.Azure
{
    public abstract class QueueProcessor<TMessageParams> : IProcessQueues
    {
        private QueueClient receiveClient;
        private static QueueClient sendClient;
        private static QueueClient errorClient;

        private const string MESSAGE_PROPERTY_KEY_MESSAGE_NAME = "MessageName";
        private const string ERROR_QUEUE_NAME = "ERRORS";

        protected QueueProcessor(string queueName)
        {
            sendClient = QueueClient.CreateFromConnectionString(Settings.ServiceBusConnectionString, queueName);
            errorClient = QueueClient.CreateFromConnectionString(Settings.ServiceBusConnectionString, ERROR_QUEUE_NAME);

            // Create the topic if it does not exist already
            string connectionString = Settings.ServiceBusConnectionString;

            var namespaceManager =
                NamespaceManager.CreateFromConnectionString(connectionString);

            if (!namespaceManager.QueueExists(queueName))
            {
                var queueDescription = new QueueDescription(queueName);
                queueDescription.LockDuration = TimeSpan.FromMinutes(5.0);
                namespaceManager.CreateQueue(queueDescription);
            }
            /*
            if (!namespaceManager.TopicExists(topic))
            {
                namespaceManager.CreateTopic(topic);
            }
            if (!namespaceManager.SubscriptionExists(topic, subscription))
            {
                SqlFilter applicationInstanceFilter = new SqlFilter(String.Format("{0} = '{1}'", MESSAGE_PROPERTY_KEY_MESSAGE_NAME, subscription));
                namespaceManager.CreateSubscription(topic, subscription, applicationInstanceFilter);
            }*/

            receiveClient = QueueClient.CreateFromConnectionString(connectionString, queueName, ReceiveMode.PeekLock);
        }

        protected enum MessageProcessStatus
        {
            Complete, ReprocessLater, ReprocessImmediately, Broken
        }

        protected virtual TMessageParams ParseMessageParams(IDictionary<string, object> messageParamsDictionary)
        {
            var constructorInfo = typeof(TMessageParams).GetConstructor(new Type[] { });
            TMessageParams messageParams = (TMessageParams)constructorInfo.Invoke(new object[] { });
            foreach (var propertyName in messageParamsDictionary.Keys)
            {
                var propertyInfo = typeof(TMessageParams).GetProperty(propertyName);
                if (propertyInfo != null) // Ignore extra fields at get stuffed into properties like "MessageName"
                {
                    var propertyValue = messageParamsDictionary[propertyName];
                    propertyInfo.SetValue(messageParams, propertyValue);
                }
            }
            return messageParams;
        }

        protected static void EncodeMessageParams(IDictionary<string, object> messageParamsDictionary, TMessageParams messageParams)
        {
            foreach (var propertyInfo in typeof(TMessageParams).GetProperties())
            {
                var propertyName = propertyInfo.Name;
                var propertyValue = propertyInfo.GetValue(messageParams);
                messageParamsDictionary[propertyName] = propertyValue;
            }
        }

        protected abstract void ProcessMessage(TMessageParams messageParams);

        public void Execute()
        {
            BrokeredMessage message;
            try
            {
                message = receiveClient.Receive();
            }
            catch (MessagingCommunicationException ex)
            {
                return;
            }
            catch (AuthorizationFailedException)
            {
                // TODO: Figure out what's causing this
                return;
            }
            catch (UnauthorizedAccessException)
            {
                // TODO: Figure out what's causing this
                return;
            }

            if (message != null)
            {
                try
                {
                    var messageParams = ParseMessageParams(message.Properties);
                    ProcessMessage(messageParams);
                    message.Complete();
                }
                catch (ReprocessMessageException)
                {
                    // TODO: Update resent count and send error if it gets too large? Maybe not,
                    // since this could be  polling strategy by an implementation.
                    // Let message get resent
                }
                catch (BrokenMessageException bmex)
                {
                    TerminateMessage(message, bmex);
                }
                catch (WebException)
                {
                    // Let message get resent
                }
                catch (Exception ex)
                {
                    // Indicate a problem, unlock message in subscription
                    try
                    {
                        TerminateMessage(message, ex);
                    }
                    catch (Exception)
                    {
                        try
                        {
                            message.DeadLetter();
                        }
                        catch (Exception)
                        {
                            try
                            {
                                message.Abandon();
                            }
                            catch (Exception)
                            {
                                // TOTAL FAIL!!!
                            }
                        }
                    }
                }
            }
        }

        private void TerminateMessage(BrokeredMessage message, Exception ex)
        {
            // Send an error message before sticking it on the dead letter queue
            var errorMessage = new BrokeredMessage();
            errorMessage.Properties.Add("message_id", message.MessageId);

            // Add the error data conservatively converting everything to strings
            foreach (var errorDataKey in ex.Data.Keys)
            {
                var errorDataValue = ex.Data[errorDataKey];
                errorMessage.Properties.Add(errorDataKey.ToString(), errorDataValue.ToString());
            }

            // Serialze other collections
            var stream = new System.IO.MemoryStream();

            // store message properties
            new System.Runtime.Serialization.Json.DataContractJsonSerializer(typeof(IDictionary<string, object>)).WriteObject(stream, message.Properties);
            var messageProperties = System.Text.Encoding.UTF8.GetString(stream.GetBuffer());
            errorMessage.Properties.Add("message_properties", messageProperties);

            // store inner exception
            try
            {
                new System.Runtime.Serialization.Json.DataContractJsonSerializer(typeof(Exception)).WriteObject(stream, ex.InnerException);
                var innerException = System.Text.Encoding.UTF8.GetString(stream.GetBuffer());
                errorMessage.Properties.Add("inner_exception", innerException);
            } catch(Exception)
            {
                // This is optional anyway
            }

            lock (errorClient)
            {
                errorClient.Send(errorMessage);
            }

            message.DeadLetter(ex.Message, errorMessage.MessageId);
        }

        protected static void Send(TMessageParams messageParams, string subscription)
        {
            var message = new BrokeredMessage();
            EncodeMessageParams(message.Properties, messageParams);
            message.Properties[MESSAGE_PROPERTY_KEY_MESSAGE_NAME] = subscription;
            lock (sendClient)
            {
                sendClient.Send(message);
            }
        }
    }
}