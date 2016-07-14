using NetMQ;
using System;

namespace MDPCommons
{
    public class MDPReplyEventArgs : EventArgs
    {
        public Guid RequestId { get; private set; }
        public string ServiceName { get; private set; }
        public NetMQMessage Reply { get; private set; }

        public Exception Exception { get; private set; }

        public MDPReplyEventArgs(Guid requestId, string serviceName, NetMQMessage reply)
        {
            Reply = reply;
            RequestId = requestId;
            ServiceName = serviceName;
        }

        public MDPReplyEventArgs(Guid requestId, string serviceName, NetMQMessage reply, Exception exception)
            : this(requestId, serviceName, reply)
        {
            Exception = exception;
        }

        public bool HasError()
        {
            return (Exception != null ? true : false);
        }
    }
}
