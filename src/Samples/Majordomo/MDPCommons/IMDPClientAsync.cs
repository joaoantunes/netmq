using System;
using NetMQ;

namespace MDPCommons
{
    public interface IMDPClientAsync : IDisposable
    {
        /// <summary>
        ///     sets or gets the timeout period that a client can stay connected 
        ///     without receiving any messages from broker
        /// </summary>
        TimeSpan Timeout { get; set; }

        /// <summary>
        ///     returns the address of the broker the client is connected to
        /// </summary>
        string Address { get; }

        /// <summary>
        ///     returns the name of the client
        /// </summary>
        byte[] Identity { get; }

        /// <summary>
        ///     send a async request to a broker for a specific service
        /// </summary>
        /// <param name="serviceName">the name of the service requested</param>
        /// <param name="request">the request message to process by service</param>
        Guid Send (string serviceName, NetMQMessage request, SendOptions options = null);

        /// <summary>
        ///  reply to an asyncronous request
        /// </summary>
        event EventHandler<MDPReplyEventArgs> ReplyReady;

        event EventHandler<MDPFailedRequestEventArgs> FailedRequest;

        /// <summary>
        ///     broadcast logging info via this event
        /// </summary>
        event EventHandler<MDPLogEventArgs> LogInfoReady;
    }
}