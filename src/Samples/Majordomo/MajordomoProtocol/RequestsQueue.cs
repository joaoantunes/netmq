using MDPCommons;
using NetMQ;
using System;
using System.Collections.Generic;
using System.Linq;

namespace MajordomoProtocol
{
    public class RequestsQueue
    {
        private object locker = new object();

        private Queue<Request> m_failoverRequests; // FIFO requests that were not sent in the proper time and need to be resend

        private Queue<Request> m_pendingRequests; // FIFO requests that were not yet sent

        private List<Request> m_outstandingRequests; // Requets that were sent but there is not yet a delivery confirmation

        public event EventHandler<MDPFailedRequestEventArgs> FailedRequest;

        public RequestsQueue()
        {
            m_failoverRequests = new Queue<Request>();
            m_pendingRequests = new Queue<Request>();
            m_outstandingRequests = new List<Request>();
        }

        public Guid EnqueueRequest(Request request)
        {
            lock (locker)
            {
                m_pendingRequests.Enqueue(request);
            }
            return request.RequestId;
        }

        public Request DequeueRequest()
        {
            Request result = null;
            lock (locker)
            {
                if (m_failoverRequests.Count > 0)
                {
                    result = m_failoverRequests.Dequeue();
                }
                else if (m_pendingRequests.Count > 0)
                {
                    result = m_pendingRequests.Dequeue();
                }
            }

            if (result != null)
            {
                AddOutstandingRequest(result);
            }
            return result;
        }

        public void PurgeRequests()
        {
            lock (locker)
            {
                var timedOutRequests = m_outstandingRequests.Where(x => x.IsTimeout).ToList();

                foreach (var req in timedOutRequests)
                {
                    if (!TrySetToBeResend(req))
                    {
                        var e = new MDPFailedRequestEventArgs(req.RequestId, req.ServiceName, req.Payload);
                        OnFailedRequest(e);
                    }
                }
            }
        }

        public bool ExistsOutstandingRequest()
        {
            return m_failoverRequests.Count > 0 || m_outstandingRequests.Count > 0;
        }

        private bool TrySetToBeResend(Request request) // TO IMPROVE! maybe use batches to add/remove to improve performance!
        {
            var retry = false;
            if (!m_outstandingRequests.Remove(request))
            {
                throw new ArgumentException($"Request {request.RequestId} was not found");
            }

            if (request.IsRepeatable)
            {
                m_failoverRequests.Enqueue(request);
                retry = true;
            }

            return retry;
        }

        public Request ConcludeRequest(Guid requestId)
        {
            // TO IMPROVE: If a reply is received and he is on m_failoverRequests, is not going to remove it, and we are going to send again the request
            //|| m_failoverRequests.RemoveAll(x => x.RequestId == requestId) > 0;
            // need to change m_failoverRequests to another structure!?

            // TO IMPROVE: I have RequestID as a key... maybe I should use a Dictionary
            lock (locker)
            {
                var req = m_outstandingRequests.FirstOrDefault(x => x.RequestId == requestId);
                if (req != null)
                {
                    m_outstandingRequests.Remove(req);
                }
                return req;
            }
        }

        private void AddOutstandingRequest(Request request)
        {
            if (request == null)
            {
                throw new ArgumentNullException("Request is required can't be null!");
            }

            request.UpdateAttempt();
            lock (locker)
            {
                m_outstandingRequests.Add(request);
            }
        }

        private void OnFailedRequest(MDPFailedRequestEventArgs e)
        {
            FailedRequest?.Invoke(this, e);
        }
    }
}
