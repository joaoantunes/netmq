using NetMQ;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MDPCommons
{
    public class MDPFailedRequestEventArgs
    {
        public string ServiceName { get; private set; }
        public Guid RequestId { get; private set; }
        public NetMQMessage Request { get; private set; }

        //public int TimeoutTime { get; private set; }

        public MDPFailedRequestEventArgs(Guid requestId, string serviceName, NetMQMessage request)
        {
            Request = request;
            RequestId = requestId;
            ServiceName = serviceName;
        }
    }
}
